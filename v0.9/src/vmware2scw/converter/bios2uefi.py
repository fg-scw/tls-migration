"""Convert a BIOS/MBR disk image to UEFI/GPT boot.

Scaleway instances use UEFI firmware. VMware VMs often use BIOS/MBR.

Strategy:
1. Resize qcow2 +200MB (host-side)
2. Fix GPT backup header with sgdisk -e via qemu-nbd (host-side)  
3. Create ESP partition via qemu-nbd + sgdisk (host-side)
4. Format ESP as FAT32 via qemu-nbd (host-side)
5. Install grub-efi inside guest via virt-customize (guest-side)
"""

import json
import logging
import os
import subprocess
import time
from pathlib import Path

logger = logging.getLogger(__name__)

ENV = {"LIBGUESTFS_BACKEND": "direct"}


def _run(cmd, check=True, **kwargs):
    """Run a command, optionally raise on failure."""
    logger.info(f"  $ {' '.join(cmd)}")
    result = subprocess.run(
        cmd, capture_output=True, text=True,
        env={**os.environ, **ENV}, **kwargs,
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            f"Command failed ({' '.join(cmd[:4])}): "
            f"{result.stderr.strip()[-500:]}"
        )
    return result


def detect_boot_type(qcow2_path: str) -> str:
    """Detect if disk uses BIOS or UEFI boot.

    Returns: 'uefi', 'bios-gpt', or 'bios-mbr'
    """
    # Use guestfish to check partition type
    result = subprocess.run(
        ["guestfish", "--ro", "-a", qcow2_path, "--",
         "run", ":", "part-get-parttype", "/dev/sda"],
        capture_output=True, text=True,
        env={**os.environ, **ENV},
    )
    part_type = result.stdout.strip()  # "msdos" or "gpt"

    if part_type == "gpt":
        # Check for EFI System Partition by listing partitions and their GUIDs
        # ESP has GUID type C12A7328-F81F-11D2-BA4B-00A0C93EC93B
        result2 = subprocess.run(
            ["guestfish", "--ro", "-a", qcow2_path, "--",
             "run", ":", "part-list", "/dev/sda"],
            capture_output=True, text=True,
            env={**os.environ, **ENV},
        )
        # Also check via sgdisk for EF00 type
        result3 = subprocess.run(
            ["guestfish", "--ro", "-a", qcow2_path, "--",
             "run"],
            input="", capture_output=True, text=True,
            env={**os.environ, **ENV},
        )
        # Check each partition for EFI type
        # Use part-get-gpt-type for each partition
        for part_num in range(1, 10):
            dev = f"/dev/sda{part_num}"
            res = subprocess.run(
                ["guestfish", "--ro", "-a", qcow2_path, "--",
                 "run", ":", "part-get-gpt-type", "/dev/sda", str(part_num)],
                capture_output=True, text=True,
                env={**os.environ, **ENV},
            )
            guid = res.stdout.strip().upper()
            if guid == "C12A7328-F81F-11D2-BA4B-00A0C93EC93B":
                logger.info(f"Found EFI System Partition at partition {part_num}")
                return "uefi"
            if res.returncode != 0:
                break  # No more partitions

        # Also check Linux-style /boot/efi mount
        result4 = subprocess.run(
            ["guestfish", "--ro", "-a", qcow2_path, "-i", "--", "mountpoints"],
            capture_output=True, text=True,
            env={**os.environ, **ENV},
        )
        if "/boot/efi" in result4.stdout:
            return "uefi"

        return "bios-gpt"
    elif part_type in ("msdos", "dos"):
        return "bios-mbr"
    else:
        logger.warning(f"Unknown partition type: '{part_type}'")
        return "bios-mbr"


def _nbd_connect(qcow2_path: str, nbd_dev: str = "/dev/nbd0") -> str:
    """Connect qcow2 to an NBD device. Returns the device path."""
    # Load nbd kernel module
    _run(["modprobe", "nbd", "max_part=16"], check=False)
    # Disconnect if already connected
    _run(["qemu-nbd", "--disconnect", nbd_dev], check=False)
    time.sleep(0.5)
    # Connect
    _run(["qemu-nbd", "--connect", nbd_dev, qcow2_path])
    time.sleep(1)  # Wait for device to settle
    # Force kernel to re-read partitions
    _run(["partprobe", nbd_dev], check=False)
    time.sleep(0.5)
    return nbd_dev


def _nbd_disconnect(nbd_dev: str = "/dev/nbd0"):
    """Disconnect NBD device."""
    _run(["qemu-nbd", "--disconnect", nbd_dev], check=False)
    time.sleep(0.5)


def convert_bios_to_uefi(qcow2_path: str, os_family: str = "linux") -> bool:
    """Convert a BIOS disk to UEFI boot. Returns True if conversion was done."""

    boot_type = detect_boot_type(qcow2_path)
    logger.info(f"Detected boot type: {boot_type}")

    if boot_type == "uefi":
        logger.info("Disk already has UEFI boot — no conversion needed")
        return False

    if os_family == "windows":
        logger.warning("Windows BIOS→UEFI not supported in fallback mode")
        return False

    logger.info("=== Phase 1: Host-side disk operations ===")

    # Step 1: Resize qcow2 to add space for ESP
    ESP_SIZE_MB = 200
    _run(["qemu-img", "resize", qcow2_path, f"+{ESP_SIZE_MB}M"])
    logger.info(f"Resized qcow2 by +{ESP_SIZE_MB}MB")

    # Step 2-4: Use qemu-nbd for partition operations on the host
    nbd_dev = "/dev/nbd0"
    try:
        _nbd_connect(qcow2_path, nbd_dev)

        # Fix GPT backup header (must be at end of disk after resize)
        logger.info("Fixing GPT backup header...")
        if boot_type == "bios-gpt":
            _run(["sgdisk", "-e", nbd_dev])
        elif boot_type == "bios-mbr":
            logger.info("Converting MBR → GPT...")
            _run(["sgdisk", "--mbrtogpt", nbd_dev])

        # Re-read partition table
        _run(["partprobe", nbd_dev], check=False)
        time.sleep(0.5)

        # Find last partition number
        result = _run(["sgdisk", "-p", nbd_dev])
        lines = [l for l in result.stdout.split('\n') if l.strip() and l.strip()[0].isdigit()]
        if not lines:
            raise RuntimeError("No partitions found on disk")
        last_part = int(lines[-1].split()[0])
        new_part = last_part + 1
        logger.info(f"Last partition: {last_part}, creating ESP as partition {new_part}")

        # Create ESP partition at end of disk
        _run([
            "sgdisk",
            f"-n{new_part}:0:+{ESP_SIZE_MB}M",
            f"-t{new_part}:EF00",
            f"-c{new_part}:EFI-System",
            nbd_dev,
        ])
        logger.info(f"Created ESP partition {new_part}")

        # Re-read partitions
        _run(["partprobe", nbd_dev], check=False)
        time.sleep(1)

        # Find the ESP device
        esp_dev = f"{nbd_dev}p{new_part}"
        if not Path(esp_dev).exists():
            # Try without 'p' separator
            esp_dev = f"{nbd_dev}{new_part}"
        if not Path(esp_dev).exists():
            raise RuntimeError(f"ESP device not found: tried {nbd_dev}p{new_part} and {nbd_dev}{new_part}")

        logger.info(f"Formatting {esp_dev} as FAT32...")
        _run(["mkfs.vfat", "-F", "32", "-n", "ESP", esp_dev])

    finally:
        _nbd_disconnect(nbd_dev)

    logger.info("=== Phase 2: Guest-side GRUB EFI installation ===")

    # Step 5: Install grub-efi inside the guest
    # The ESP partition now exists on disk, virt-customize can see it
    grub_script = _build_grub_efi_script(new_part)

    _run([
        "virt-customize", "-a", qcow2_path,
        "--install", "grub-efi-amd64,grub-efi-amd64-bin,dosfstools",
        "--run-command", grub_script,
    ])

    logger.info("BIOS → UEFI conversion complete")
    return True


def _build_grub_efi_script(esp_part_num: int) -> str:
    """Build script to install GRUB EFI inside the guest."""
    return f'''#!/bin/bash
set -e
echo "=== Installing GRUB EFI ==="

# Find the ESP partition
DISK="/dev/sda"
ESP_DEV="${{DISK}}{esp_part_num}"
if [ ! -b "$ESP_DEV" ]; then
    ESP_DEV="${{DISK}}p{esp_part_num}"
fi
echo "ESP device: $ESP_DEV"

# Mount ESP
mkdir -p /boot/efi
mount "$ESP_DEV" /boot/efi

# Add to fstab
ESP_UUID=$(blkid -o value -s UUID "$ESP_DEV")
if [ -n "$ESP_UUID" ]; then
    sed -i '\\|/boot/efi|d' /etc/fstab
    echo "UUID=$ESP_UUID /boot/efi vfat umask=0077 0 1" >> /etc/fstab
fi

# Install GRUB EFI
grub-install --target=x86_64-efi --efi-directory=/boot/efi --bootloader-id=ubuntu --recheck --no-floppy 2>&1 || \\
grub-install --target=x86_64-efi --efi-directory=/boot/efi --bootloader-id=BOOT --recheck --no-floppy 2>&1 || {{
    echo "grub-install failed, manual EFI setup..."
    mkdir -p /boot/efi/EFI/BOOT
    cp /usr/lib/grub/x86_64-efi/monolithic/grubx64.efi /boot/efi/EFI/BOOT/BOOTX64.EFI 2>/dev/null || true
}}

# Create fallback EFI boot path
mkdir -p /boot/efi/EFI/BOOT
if [ -f /boot/efi/EFI/ubuntu/grubx64.efi ]; then
    cp /boot/efi/EFI/ubuntu/grubx64.efi /boot/efi/EFI/BOOT/BOOTX64.EFI
elif [ -f /boot/efi/EFI/ubuntu/shimx64.efi ]; then
    cp /boot/efi/EFI/ubuntu/shimx64.efi /boot/efi/EFI/BOOT/BOOTX64.EFI
fi

# Enable serial console for Scaleway
if [ -f /etc/default/grub ]; then
    sed -i 's/^GRUB_CMDLINE_LINUX_DEFAULT=.*/GRUB_CMDLINE_LINUX_DEFAULT="console=tty1 console=ttyS0,115200n8"/' /etc/default/grub
fi

# Regenerate GRUB config
grub-mkconfig -o /boot/grub/grub.cfg 2>/dev/null || true

umount /boot/efi 2>/dev/null || true
echo "=== GRUB EFI installation complete ==="
'''

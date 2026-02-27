# vmware2scw — VMware to Scaleway Instance Migration Tool

Outil CLI pour automatiser la migration de machines virtuelles depuis un environnement **VMware vSphere/vCenter** vers des **Instances Scaleway** (KVM/qcow2).

## Prérequis

### Système

```bash
apt-get install -y qemu-utils libguestfs-tools guestfs-tools nbdkit \
    linux-image-generic ntfs-3g ovmf python3 python3-pip python3-venv
```

### Windows VMs (optionnel)

```bash
# Télécharger les drivers VirtIO pour Windows
wget -O /opt/virtio-win.iso \
  https://fedorapeople.org/groups/virt/virtio-win/direct-downloads/stable-virtio/virtio-win.iso
```

### Python

```bash
python3 -m venv venv
source venv/bin/activate
pip install -e .
```

## Configuration

Copier et éditer le fichier de configuration :

```bash
cp configs/example_config.yaml migration.yaml
```

```yaml
# migration.yaml
vmware:
  vcenter: vcenter.local
  username: administrator@vsphere.local
  password_env: VCENTER_PASSWORD      # ou password: "direct" (déconseillé)
  insecure: true                       # SSL auto-signé

scaleway:
  access_key_env: SCW_ACCESS_KEY
  secret_key_env: SCW_SECRET_KEY
  organization_id: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
  project_id: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
  default_zone: fr-par-1
  s3_region: fr-par
  s3_bucket: vmware-migration-transit

conversion:
  work_dir: /var/lib/vmware2scw/work
  compress_qcow2: true
  virtio_win_iso: /opt/virtio-win.iso  # Requis pour les VMs Windows
  cleanup_on_success: true
```

## Quick Start

```bash
# 1. Inventaire + plan de migration
vmware2scw inventory-plan --config migration.yaml \
  --filter "os:linux" --auto-map -o plan.yaml

# 2. Vérifier et éditer plan.yaml (types cibles, priorités, waves)

# 3. Estimation pré-migration
vmware2scw batch estimate --plan plan.yaml

# 4. Exécution
vmware2scw batch run --plan plan.yaml --config migration.yaml
```

---

## Commandes

### `vmware2scw inventory` — Inventaire basique

Liste toutes les VMs d'un vCenter.

```bash
# Affichage tableau
vmware2scw inventory \
  --vcenter vcenter.local \
  --username administrator@vsphere.local \
  --password-file /root/.vcenter_pass \
  --insecure

# Export JSON
vmware2scw inventory \
  --vcenter vcenter.local \
  --username administrator@vsphere.local \
  --password "MonMotDePasse" \
  --insecure \
  --format json \
  -o inventory.json
```

**Options :**
| Option | Description |
|--------|-------------|
| `--vcenter` | Hostname ou IP du vCenter |
| `--username` | Utilisateur vCenter |
| `--password` / `--password-file` | Mot de passe (fichier recommandé) |
| `--insecure` | Ignorer la vérification SSL |
| `--format table\|json` | Format de sortie |
| `-o` / `--output` | Fichier de sortie |

---

### `vmware2scw inventory-plan` — Inventaire filtré + auto-mapping

Liste les VMs avec filtrage avancé et mapping automatique vers les types Scaleway.
Génère un plan de migration YAML prêt pour exécution.

```bash
# Toutes les VMs Linux avec auto-mapping
vmware2scw inventory-plan --config migration.yaml \
  --filter "os:linux" --auto-map -o plan.yaml

# Serveurs web de production avec >4 CPUs
vmware2scw inventory-plan --config migration.yaml \
  -f "name:web-prod-*" -f "os:linux" --min-cpu 4 \
  --sizing optimize -o plan.yaml

# VMs Windows dans un dossier spécifique
vmware2scw inventory-plan --config migration.yaml \
  -f "folder:/DC1/Production/Windows" -o windows-plan.yaml

# VMs allumées sur des hôtes ESXi spécifiques
vmware2scw inventory-plan --config migration.yaml \
  -f "state:poweredOn" -f "host:esxi-0[12]*" \
  --auto-map --tag migrated-from-vmware -o plan.yaml

# Affichage YAML sans sauvegarder
vmware2scw inventory-plan --config migration.yaml \
  --filter "os:linux" --format yaml
```

**Filtres disponibles :**

| Filtre | Exemple | Description |
|--------|---------|-------------|
| `name:` | `name:web-*` | Glob pattern sur le nom VM |
| `regex:` | `regex:^prod-\d+` | Regex sur le nom VM |
| `folder:` | `folder:/DC1/Production` | Préfixe dossier vCenter |
| `os:` | `os:linux`, `os:windows` | Famille OS |
| `host:` | `host:esxi-01*` | Hôte ESXi (glob) |
| `cluster:` | `cluster:prod-*` | Cluster vCenter |
| `dc:` | `dc:DC1` | Datacenter |
| `state:` | `state:poweredOn` | État d'alimentation |
| `firmware:` | `firmware:bios` | Type firmware |
| `--min-cpu` | `--min-cpu 4` | vCPU minimum |
| `--max-cpu` | `--max-cpu 16` | vCPU maximum |
| `--min-ram` | `--min-ram 8` | RAM min (GB) |
| `--max-disk` | `--max-disk 500` | Disque total max (GB) |

**Options de sizing :**
| Stratégie | Description |
|-----------|-------------|
| `exact` | Type le plus proche des specs source |
| `optimize` | Right-sizing avec marge (recommandé) |
| `cost` | Type le moins cher viable |

---

### `vmware2scw batch estimate` — Estimation pré-migration

Analyse le plan et fournit des estimations avant exécution.

```bash
vmware2scw batch estimate --plan plan.yaml
vmware2scw batch estimate --plan plan.yaml --available-disk 10000
```

Affiche : espace disque nécessaire, durée estimée, coût mensuel Scaleway, avertissements.

---

### `vmware2scw batch run` — Exécution batch avec dashboard

Exécute le plan de migration en parallèle avec gestion par waves.

```bash
# Interactif (confirmation demandée)
vmware2scw batch run --plan plan.yaml --config migration.yaml

# Non-interactif
vmware2scw batch run --plan plan.yaml --config migration.yaml -y

# Dry run (simulation sans exécution)
vmware2scw batch run --plan plan.yaml --config migration.yaml --dry-run

# Avec rapport de sortie
vmware2scw batch run --plan plan.yaml --config migration.yaml \
  --report report.md
```

---

### `vmware2scw batch resume` — Reprise après échec

Reprend les VMs échouées depuis leur dernier stage réussi.

```bash
vmware2scw batch resume --batch-id abc12345 --config migration.yaml
```

---

### `vmware2scw batch status` — État d'un batch

```bash
vmware2scw batch status                          # Batch le plus récent
vmware2scw batch status --batch-id abc12345      # Batch spécifique
```

---

### `vmware2scw batch report` — Rapport post-migration

```bash
vmware2scw batch report --batch-id abc12345 -o report.md
vmware2scw batch report --batch-id abc12345      # Affichage stdout
```

---

### `vmware2scw migrate` — Migration individuelle

Migre une seule VM (sans plan batch).

```bash
vmware2scw migrate \
  --vm "web-server-01" \
  --target-type POP2-4C-16G \
  --zone fr-par-1 \
  --config migration.yaml

# Dry run
vmware2scw migrate \
  --vm "web-server-01" \
  --target-type POP2-4C-16G \
  --config migration.yaml \
  --dry-run
```

---

## Pipeline de migration

Chaque VM passe par les étapes suivantes :

### Linux (9 étapes)

| # | Étape | Description |
|---|-------|-------------|
| 1 | `validate` | Vérification compatibilité VM / type cible |
| 2 | `snapshot` | Snapshot VMware pour cohérence des données |
| 3 | `export` | Export des disques VMDK depuis VMware |
| 4 | `convert` | Conversion VMDK → qcow2 (`qemu-img`) |
| 5 | `adapt_guest` | Nettoyage VMware tools + VirtIO + bootloader + réseau (1 seul appel `virt-customize`) |
| 6 | `ensure_uefi` | Conversion BIOS → UEFI si nécessaire |
| 7 | `upload_s3` | Upload qcow2 vers Scaleway Object Storage |
| 8 | `import_scw` | Import image Scaleway (snapshot → image) |
| 9 | `verify` | Vérification post-migration |
| 10 | `cleanup` | Nettoyage fichiers temporaires et snapshots |

### Windows (12 étapes)

| # | Étape | Description |
|---|-------|-------------|
| 1 | `validate` | Vérification compatibilité |
| 2 | `snapshot` | Snapshot VMware |
| 3 | `export` | Export VMDK |
| 4 | `convert` | VMDK → qcow2 (non compressé pour Windows) |
| 5 | `clean_tools` | Nettoyage VMware tools |
| 6 | `inject_virtio` | Installation drivers VirtIO (Phase 1 offline + Phase 2 QEMU boot) |
| 7 | `fix_bootloader` | Adaptation bootloader KVM |
| 8 | `ensure_uefi` | BIOS → UEFI (MBR→GPT + bcdboot) |
| 9 | `upload_s3` | Upload S3 |
| 10 | `import_scw` | Import Scaleway |
| 11 | `verify` | Vérification |
| 12 | `cleanup` | Nettoyage |

---

## Format du plan batch (YAML)

```yaml
version: 1

metadata:
  generated_at: "2026-02-27T14:00:00Z"
  vcenter: vcenter.local

defaults:
  zone: fr-par-1
  sizing_strategy: optimize
  tags:
    - migrated-from-vmware

concurrency:
  max_exports_per_host: 4
  max_concurrent_conversions: 3
  max_concurrent_uploads: 6
  max_total_workers: 10

migrations:
  - vm_name: web-dev-01
    target_type: PLAY2-MICRO
    priority: 1
    wave: canary

  - vm_pattern: "dev-*"
    target_type: PLAY2-NANO
    priority: 3
    wave: dev

  - vm_name: web-prod-01
    target_type: POP2-4C-16G
    priority: 2
    wave: production

exclude:
  - vm_pattern: "template-*"
  - vm_name: legacy-do-not-touch

waves:
  - name: canary
    vms: ["web-dev-01"]
    pause_after: pause              # Toujours attendre

  - name: dev
    vms: ["dev-*"]
    pause_after: pause_on_failure   # Continuer sauf erreur

  - name: production
    vms: ["web-prod-*", "db-*"]
    pause_after: pause

post_migration:
  tag_source: "migrated-to-scaleway"
  power_off_source: false
  delete_vmware_snapshot: true
  delete_s3_transit: true
```

---

## Types d'instances Scaleway supportés

| Catégorie | Types | Usage |
|-----------|-------|-------|
| Development | PLAY2-NANO, PLAY2-MICRO, PLAY2-SMALL | Dev/test, vCPU partagés |
| General Purpose | PRO2-XXS à PRO2-L | Production générale |
| Compute (NVMe) | POP2-2C-8G à POP2-32C-128G | Workloads CPU-intensive |
| High-Memory | POP2-HM-2C-16G à POP2-HM-64C-512G | Bases de données, caches |
| Windows | POP2-*-WIN, POP2-HM-*-WIN | Windows Server |

---

## Docker

```bash
docker build -t vmware2scw .
docker run --rm -v $(pwd)/migration.yaml:/app/migration.yaml \
  vmware2scw inventory --config /app/migration.yaml --insecure
```

---

## Développement

```bash
# Installation dev
pip install -e ".[dev]"

# Tests
python -m pytest vmware2scw/tests/ -v

# Linting
ruff check vmware2scw/
mypy vmware2scw/
```

---

## Limitations connues

- **BIOS → UEFI Windows** : nécessite KVM (`/dev/kvm`) et OVMF sur l'hôte d'orchestration
- **Disques RDM** (Raw Device Mapping) : non supportés — détectés en pré-validation
- **GPU passthrough** : non migreable automatiquement
- **Licences Windows** : l'activation peut être perdue après changement de hardware
- **Snapshots VMware complexes** : consolider avant migration recommandé

## Licence

Apache 2.0

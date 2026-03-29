NVIDIA dataset structure and score schema

==========================================

Directory Structure
==========================================

The mapping file is stored alongside the data directories at the same level:

nvda/
├── model_entity_metadata_mapping.csv   <- Mapping file (chunk number only applicable for models)
└── msas/                               <- Multiple sequence alignments
    ├── 251124_1.2M
    ├── 251124_8.8M
    └── 251209_13.4M
└── models/                             <- Model data
    ├── chunk_0000.tar
    ├── chunk_0001.tar
    .........
    ├── chunk_3998.tar
    └── chunk_3999.tar

This file maps modelEntityId to their taxonomy details and the chunked tar file.

Note: We are actively working on grouping models by organism. An updated dataset with this structure will be available soon.
All files in these directories use zstd compression.
Individual files within the tarballs are separately compressed.
Each tar file is self-contained and independent.

==========================================
Score schema
==========================================

The metadata table includes entity identifiers, taxonomy fields, interface-confidence scores, supporting ipSAE-derived fields, interaction counts, and clash-based quality-control fields. In the descriptions below, the focus is on the simplified right-hand-side CSV headers.

Directional fields use the suffixes _AB and _BA. _AB refers to the ordered calculation for chain A against chain B, whereas _BA refers to the same type of calculation in the reverse direction.

Field descriptions for CSV headers

----------------------------------------
Entity identifiers

CSV column                  Meaning
--------------------------------------------------------------------------------
modelEntityId               AlphaFold DB model entity identifier for the predicted complex.
uniprotAccession            UniProt accession associated with the entry.
gene                        Gene name associated with the entry, where available.
taxId                       NCBI taxonomy identifier for the source organism.
organismScientificName      Scientific name of the source organism.
chunk                       Name of the chunked tar file.

----------------------------------------
Primary confidence scores

CSV column                  Meaning
--------------------------------------------------------------------------------
ipTM                        Main interface TM-like confidence score retained in this schema. This is the single general ipTM field used for the dataset.
ipSAE_AB                    ipSAE score for the A -> B chain order.
ipSAE_BA                    ipSAE score for the B -> A chain order.
pDockQ                      Global pDockQ score for the interface.
pDockQ2_AB                  Directional pDockQ2 score for A -> B.
pDockQ2_BA                  Directional pDockQ2 score for B -> A.
LIS_AB                      Local Interaction Score for the A -> B direction.
LIS_BA                      Local Interaction Score for the B -> A direction.
ipTM_d0chn_AB               Directional interface TM-like score using the d0chn scaling term for A -> B.
ipTM_d0chn_BA               Directional interface TM-like score using the d0chn scaling term for B -> A.

----------------------------------------
Supporting ipSAE-derived fields

These fields support the directional interface calculations and help explain how the directional scores were derived.

CSV column                  Meaning
--------------------------------------------------------------------------------
n0chn                       Chain-level reference size term used in the ipSAE-related scaling.
n0res_AB                    Residue-count reference term used in the residue-level calculation for A -> B.
n0dom_AB                    Domain-count reference term used in the domain-level calculation for A -> B.
d0res_AB                    Residue-level distance scaling term used in the calculation for A -> B.
d0chn_AB                    Chain-level distance scaling term used in the calculation for A -> B.
d0dom_AB                    Domain-level distance scaling term used in the calculation for A -> B.
nres1_AB                    Number of residues from the first chain participating in the A -> B calculation.
dist1_AB                    Number of residues from the first chain within the applied interface-distance cutoff for A -> B.
nres2_AB                    Number of residues from the second chain participating in the A -> B calculation.
dist2_AB                    Number of residues from the second chain within the applied interface-distance cutoff for A -> B.
n0res_BA                    Residue-count reference term used in the residue-level calculation for B -> A.
n0dom_BA                    Domain-count reference term used in the domain-level calculation for B -> A.
d0res_BA                    Residue-level distance scaling term used in the calculation for B -> A.
d0chn_BA                    Chain-level distance scaling term used in the calculation for B -> A.
d0dom_BA                    Domain-level distance scaling term used in the calculation for B -> A.
nres1_BA                    Number of residues from the first chain participating in the B -> A calculation.
dist1_BA                    Number of residues from the first chain within the applied interface-distance cutoff for B -> A.
nres2_BA                    Number of residues from the second chain participating in the B -> A calculation.
dist2_BA                    Number of residues from the second chain within the applied interface-distance cutoff for B -> A.

----------------------------------------
Threshold and quality-control fields

CSV column                  Meaning
--------------------------------------------------------------------------------
ipSAE_PAE_cutoff            PAE threshold applied when calculating the ipSAE-related fields.
ipSAE_dist_cutoff           Distance threshold applied when calculating the ipSAE-related fields.
numberOfInteractions        Number of detected inter-chain interactions for the model according to the dataset pipeline.
N_clash_backbone            Number of backbone atom clashes detected in the complex.
N_clash_heavyAtom           Number of heavy-atom clashes detected in the complex.

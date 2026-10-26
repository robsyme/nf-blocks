# P4 screenshots: projects and the differentialabundance hand-off

`gate/p4/web-accept.sh` took these. It serves each store with `nf-blocks:explore` and runs `shoot.mjs` from this folder. Every shot is 1440x900, in `-light` and `-dark`.

The stores were scratch copies of roblab (`/tmp/cas-exploration/store`) and the plugin half's input sandbox. Every write went through the page and the explore server, signed by `cas.asserted_by`: the developer's example.org address (RS), or ana.k@example.org (AK). `web-accept.sh` now signs as alice@example.org instead of the former, so a re-shoot shows that address.

All shots come from one run:

- Nextflow 26.10.3, with nf-blocks 0.9.0-beta.1 built from f994e8a.
- Command: `SANDBOX_SRC=<sandbox ROOT> gate/p4/web-accept.sh <ROOT> setup a ana pin handoff verify c b shots`, where `<sandbox ROOT>` is what `gate/p4/input-sandbox.sh` built.
- Stores: fresh copies of roblab and of the sandbox store, which was rebuilt at d75bb14.
- The project's retention policy was on throughout.

| Shot | What it shows | Numbers seen |
|---|---|---|
| `mock-13-project`, `mock-14-handoff-de` | Mockups 13 and 14 from `docs/design/mockups/`, with the spec panel and theme button removed | |
| `build-13-project-overview-two-stores` | Project "RAP1 depletion" Overview, served over roblab and org-local after the wigshop copy and its policy pin in org-local. Activity says "in roblab" and "Read from roblab only. The same in org-local." | roblab 142 MB, Pinned 1.35 MB, Held 141 MB; org-local 1.14 MB, Pinned 1.14 MB, Held 0 B; two bars, never summed |
| `build-13-project-pin-org-local` | The project's Selections tab over org-local alone, after the copy: D5's "1 Selection not pinned by policy · Pin", opened. The head reads "1 not pinned here" | "wigshop Pin 1.14 MB in org-local"; "Pins apply to org-local only. The note on each is "project policy: RAP1 depletion"."; button "Pin 1 Selection"; pin Claim `bafyreieyplzevw5ubzog6mxg65at4zq6v3tyqabb2xs7cllzjut5docmeu` |
| `build-13-project-samples-two-stores` | The Samples tab over both members, the closest state to mockup 13 | 5 samples, 3 runs each; reads 49,791 / 49,969 / 99,971 / 99,566 / 49,760; condition AK auxin_30m, RS uninduced x2, RS wild_type x2 |
| `build-13-project-samples` | The same tab over roblab alone. The default store reads "org-local (not configured here)", and the subline reads "pins every Selection". There is no D5 line, because every Selection is pinned in roblab | as above, one bar |
| `build-13-project-activity` | Activity, "newest first · in roblab", 16 events, including "pinned wigshop in roblab · policy" and "pinned uninduced vs wild_type (differentialabundance) in roblab · policy" | |
| `build-14-handoff-de` | Hand-off from distraught_murdock at reads (trimmed) >= 50,000 with the project active: the replicate check fails, and Saves lists the pin | pin "keeps about 140 KB in roblab"; samplesheet `bafkreihfjanx2wpx2qvm3lvkp6dh35mgm527r4idpm4bohejgoreujzwxy` |
| `build-14-handoff-de-saved` | After Include all 5 and Save. All writes succeeded, the policy pin among them | Handoff `bafyreidmrlzkrsmnqzd26qx75iauuusp3shrwf3orz2ukwonh3ik3sr2x4`; Selection `bafyreiay2flm6qyn62yf2vdv7645hinbjs37hnpusgzobx7nbctqgy35ey`; pin Claim `bafyreih6sokskgrejxxoitwry5jklubwjqc2ne67lf4sc7jmpf7ypfgkei`; samplesheet `bafkreifo5gt2dsdh2pua6psxfrdpacraot4qnhyln3bivg5man6kfalccq` (141 B) |
| `build-selection-add-and-pin` | wigshop's Add to project panel before the click: the button names the pin's bytes, and the panel says who already pins it here | "Add and pin 1.14 MB in roblab"; "1.14 MB already pinned here by ana.k@example.org" |
| `build-selection-pinned` | wigshop's page after the policy pin and the snapshot: durability Pinned, with AK's own pin and RS's policy pin. The rail's Claims read "added to RAP1 depletion" | "paper figure 2" (AK), "project policy: RAP1 depletion" (RS), each Unpin "kept only by this pin in roblab: 0 B" |
| `build-copy-default-store` | Copy to… from the hand-off's Selection, a member of the project, over roblab and org-local: the page opens with the project's default store chosen. Nothing is copied | link `#/selection/bafyreiay…/copy?to=org-local`; destination org-local; plan 174 blocks, 90 already in org-local, 84 to transfer, 224 KB |
| `build-projects-list` | Projects list over roblab | 1 project; runs 3, Selections 2; roblab 142 MB |
| `build-run-add-to-project` | distraught_murdock's head with its "added to RAP1 depletion" tag and the Projects panel open, New project… closed | |
| `build-projects-list-suggestion` | The sandbox's Projects list before any project: one suggestion | "These 3 runs share 11 input files: p4-rnaseq-2, p4-rnaseq-wt-short, p4-rnaseq-1" |
| `build-project-samples-input-reads` | The project created from that suggestion: its Samples tab, grouped by input reads, with the Input reads column | 6 samples: WT_REP1 twice (2 runs, 99,566 reads; 1 run, 19,914 reads, the truncated reads); p4sandbox 166 MB |

The first run's `build-14-handoff-de-pin-refused` showed the server refusing a pin on a Selection. d75bb14 fixed that, so the shot is gone.

To re-shoot after a web change, over the stores a full run left:

```
gate/p4/web-accept.sh <ROOT> build
SHOOT_ONLY=1 gate/p4/web-accept.sh <ROOT> shots
```

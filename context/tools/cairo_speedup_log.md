# CAIRO speedup log

Tracking benchmark results for issue #250.
Machine: 8-core EC2
Baseline: ~2.5 min/run before any patches

---

## Baseline — run 1 (pre-patch), 2026-02-23

| Stage | Time (s) |
|-------|----------|
| _load_prototype_ids_for_run | 0.1 |
| _initialize_tariffs | 0.0 |
| _build_precalc_period_mapping | 0.0 |
| return_buildingstock | 0.5 |
| build_bldg_id_to_load_filepath | 1.5 |
| _return_load(electricity) | 19.5 |
| _return_load(gas) | 20.3 |
| phase2_marginal_costs_rr | 3.5 |
| bs.simulate | 104.8 |
| **Total** | **150.2** |

# Apply CAIRO performance monkey-patches before any test runs.
# This ensures that tests which call cairo.rates_tool.loads.process_building_demand_by_period
# or cairo.rates_tool.system_revenues.run_system_revenues get the patched (vectorized) versions,
# regardless of test execution order or whether the test imports patches directly.
import utils.mid.patches  # noqa: F401

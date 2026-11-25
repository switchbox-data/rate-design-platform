"""Allocation formulas for marginal-cost 8760s.

Each module is one formula. It takes already-resolved inputs and returns one
cost per input hour. State, utility, and ISO wrappers choose the ``$/kW-yr``
and the load curve, then call the function.
"""

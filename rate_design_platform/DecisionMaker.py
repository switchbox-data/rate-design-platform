from rate_design_platform.utils.rates import TOUParameters


class BasicHumanController:
    def evaluate_TOU(
        self,
        current_state: str,
        default_bill: float,
        tou_bill: float,
        tou_comfort_penalty: float,
        TOU_params: TOUParameters,
    ) -> str:
        """
        Human controller for TOU scheduling

        Args:
            current_state: Current schedule state ("default" or "tou")
            realized_savings: Realized savings (if on TOU)
            unrealized_savings: Unrealized savings (if on default)

        Returns:
            New schedule state ("default" or "tou")
        """
        if current_state == "default":
            # Currently on default schedule, considering switch TO TOU
            anticipated_savings = default_bill - tou_bill
            net_savings = anticipated_savings - TOU_params.get_switching_cost_to()
            if net_savings > 0:
                return "switch"
            else:
                return "stay"
        else:
            # Currently on TOU schedule, considering switch BACK to default
            realized_savings = default_bill - tou_bill
            net_savings = realized_savings - TOU_params.get_switching_cost_back() - tou_comfort_penalty
            if net_savings <= 0:  # Note: condition changed to match section 2.1 logic
                return "switch"
            else:
                return "stay"

###########EXTERNAL IMPORTS############

#######################################

#############LOCAL IMPORTS#############

from controller.node import Node

#######################################


def get_node_prefix(node: Node) -> str:
    """
    Returns the phase-related prefix from a node's name.

    Checks if the name starts with a known prefix (e.g. "l1_", "l2_", "total_")
    and returns it, or an empty string if none match.
    """

    for prefix in ("l1_l2_", "l2_l3_", "l3_l1_", "l1_", "l2_", "l3_", "total_"):
        if node.name.startswith(prefix):
            return prefix
    return ""


def remove_phase_string(name: str) -> str:
    """
    Removes the phase or total prefix (e.g., 'l1_', 'l2_', 'l3_', 'total_', 'l1_l2_', etc.) from a node name if present.

    Args:
        name (str): The name of the node.

    Returns:
        str: The node name without the phase prefix.
    """

    parts = name.split("_")

    # Handle common prefixes
    if parts[0] in {"l1", "l2", "l3", "total"}:
        # Check for line-to-line voltages
        if len(parts) > 1 and parts[1] in {"l1", "l2", "l3"}:
            return "_".join(parts[2:])
        return "_".join(parts[1:])

    return name

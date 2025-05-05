from matplotlib import colors as mpl_colors


def get_nben_cmap() -> mpl_colors.LinearSegmentedColormap:
    return mpl_colors.LinearSegmentedColormap.from_list(
        "custom_cmap",
        ["chocolate", "darkviolet", "royalblue"],
    )

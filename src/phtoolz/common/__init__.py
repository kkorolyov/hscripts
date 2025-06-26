import warnings

# numpy not happy with wsl1
warnings.filterwarnings(
    action="ignore",
    category=UserWarning,
    module=r"numpy.*",
    message=r"Signature b",
)

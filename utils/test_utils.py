from config_loader import load_config
cfg = load_config("dev")
print(cfg["mongodb"]["uri"])
print(cfg["kafka"]["bootstrap_servers"])
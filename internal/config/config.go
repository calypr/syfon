package config

func LoadConfig(configFile string) (*Config, error) {
	cfg := defaultConfig()
	if err := loadConfigFile(configFile, cfg); err != nil {
		return nil, err
	}
	if err := applyEnvironmentOverrides(cfg); err != nil {
		return nil, err
	}
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	exportAuthEnvironment(cfg)
	return cfg, nil
}

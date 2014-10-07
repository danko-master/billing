require 'yaml'

module Configuration
  def self.load_config
    config_file = 'config/config.yml'
    File.exist?(config_file) ? YAML.load_file(config_file)[ENV['APP_ENV']] : {}
  end
end

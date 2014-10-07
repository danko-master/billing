#!/usr/bin/env ruby
# encoding: utf-8

## Достаточно запуска только sidekiq
# Run: export APP_ENV=development && bundle exec sidekiq -C ./config/sidekiq.yml -r ./runner.rb



if ENV['APP_ENV']
  # require 'pry'
  
  require_relative 'config/config'
  $config = Configuration.load_config

  require 'logger'
  current_logger = Logger.new("#{File.dirname(__FILE__)}/log/billing_#{ENV['APP_ENV']}.log")
  current_logger.info "Started"
  

  require_relative 'lib/workers'
  require_relative 'lib/db'
 
  require 'active_record'
  ActiveRecord::Base.establish_connection(
        :adapter  => $config['database']['adapter'],
        :database => $config['database']['database'],
        :username => $config['database']['username'],
        :password => $config['database']['password'],
        :host     => $config['database']['host'])

  
  require 'sidekiq' 
  instances = 0
  while instances < $config['runner']['instances'].to_i
    BillingWorkers::Calc.perform_async(instances)
    instances += 1
  end
else
  puts 'Error: not found "APP_ENV"!'
end
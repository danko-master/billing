module TestWorkers
  class Calc
    include Sidekiq::Worker
    sidekiq_options queue: :test

    def perform
      # status_log_file = "#{File.dirname(__FILE__)}/../../log/status_#{ENV['APP_ENV']}_inst_#{num_inst}.log" 
      # File.open(status_log_file, 'w') do |f|
      #   f.write "#{Time.now} TDR принято "
      # end
    end
  end
end
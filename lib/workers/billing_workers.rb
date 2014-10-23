require 'bson'
require 'json'
require 'bunny'
# require 'curb'

module BillingWorkers
  class Calc
    include Sidekiq::Worker
    sidekiq_options queue: :billing

    def perform(num_inst)
      @experiment_logger = []

      @bunny = Bunny.new(host: $config['rabbit']['host'], 
        port: $config['rabbit']['port'], 
        user: $config['rabbit']['user'], 
        password: $config['rabbit']['password'])
      @current_logger = Logger.new("#{File.dirname(__FILE__)}/../../log/sidekiq_#{ENV['APP_ENV']}_inst_#{num_inst}.log")
      @current_logger.info "NOTIFICATIONS: Started"      
      begin
        @current_logger.info p " [*] RUBY Waiting for messages. To exit press CTRL+C"
        @bunny.start
        @ch   = @bunny.create_channel

        time_count = Time.now.to_i
        run(num_inst, time_count)
      rescue Interrupt => _
        @bunny.close
        @current_logger.info "NOTIFICATIONS: Stopped"
        exit(0)
      end 
    end

    def run(num_inst, time_count)     
      # количество прочитанных сообщений 
      message_count_recieve = 0
      sum_count = 0  
      status_log_file = "#{File.dirname(__FILE__)}/../../log/status_#{ENV['APP_ENV']}_inst_#{num_inst}.log"        
  
      @current_logger.info p "Выполняем run, ждем tdr. input_queue #{$config['runner']['input_queue']}"  
      q    = @ch.queue($config['runner']['input_queue'], :durable => true) 
      q.subscribe(:block => true, :manual_ack => true) do |delivery_info, properties, body|
        
        # begin          
          tdr_data = Hash.new
          tdr_data['delivery_tag'] = delivery_info.delivery_tag
          tdr_data['tdr'] = body

          @current_logger.info p "Bunny ::: получили данные #{tdr_data}"

          if tdr_data.present?
            @current_logger.info p "Получен хеш tdr."
            delivery_tag = tdr_data['delivery_tag']
            
            tdr = Tdr.new(tdr_data['tdr'])
            

            if tdr.present?
              @current_logger.info p "Новый tdr #{tdr} - imei - #{tdr.imei} ::: delivery_tag #{delivery_tag}"

              # используем данные редиса, которые публикуются scheduled_jobs
              p "tdr.imei #{tdr.imei}"
              
              # заглушка вычислений, пока просто умножаем километры на 100 рублей 
              sum = tdr.path*100              
              tdr.sum = sum
              @current_logger.info p "tdr #{tdr}"
              send_tdr_data_to_rabbit(tdr)

              # отправка ack в канал
              @current_logger.info p "Отправка ack в RabbitMQ ::: delivery_tag: #{delivery_tag}"
              @ch.ack(delivery_tag)
              @current_logger.info p "Обработан tdr ::: delivery_tag #{delivery_tag} #{tdr} ::: sum #{sum} ::: #{tdr.full_info}"    

            end   
          else
            # Заглушка 
            # пока отправляем ack в любом случае
            @current_logger.info p "Принудительная Отправка ack в RabbitMQ ::: delivery_tag: #{delivery_info.delivery_tag}"
            @ch.ack(delivery_info.delivery_tag)     
          end

        # rescue Exception => e
        #   puts "ERROR! #{e}"
        # end


      end
    end

    def night_time?
      false
    end

    def send_tdr_data_to_rabbit(tdr)
      @current_logger.info p "Отправка tdr в RabbitMQ #{tdr} ::: sum: #{tdr.sum}"
      q    = @ch.queue($config['runner']['output_queue'], :durable => true)
      h = {
        # id машины
        imei: tdr.imei, 
        road_id: tdr.full_info['road_id'], 
        lat0: tdr.full_info['lat0'], 
        lon0: tdr.full_info['lon0'], 
        time0: tdr.full_info['time0'], 
        lat1: tdr.full_info['lat1'], 
        lon1: tdr.full_info['lon1'], 
        time1: tdr.full_info['time1'], 
        path: tdr.full_info['path'],
        sum: tdr.sum
      }
      tdr_doc = h.to_json
      @ch.default_exchange.publish(tdr_doc, :routing_key => q.name)
    end
  end

  class Customer
    def initialize(data)      
      @id = data.id
      @discount = data.discount
    end

    def id
      @id 
    end

    def discount
      @discount
    end
  end

  class Tdr
    def initialize(doc)
      p "TDR init"
      p doc = JSON.parse(doc)
      p @path = doc['path']
      # здесь косяк с передачей imei
      p @imei = doc['imei']
      p @full_info = doc
      p @sum
      p "TDR end init"
    end

    def imei
      @imei
    end

    def path
      @path
    end

    def full_info
      @full_info
    end

    def sum
      @sum
    end

    def sum=(sum)
      @sum = sum
    end
  end
end
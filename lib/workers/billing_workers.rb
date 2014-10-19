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

        time_count = Time.now.to_f
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

      @current_logger.info p "Выполняем run, ждем tdr."  
      q    = @ch.queue($config['runner']['input_queue'], :durable => true) 
      q.subscribe(:block => true, :manual_ack => true) do |delivery_info, properties, body|
        
        # begin          
          time1 = Time.now

          tdr_data = Hash.new
          tdr_data['delivery_tag'] = delivery_info.delivery_tag

          # tdr_data['tdr'] = BSON::Document.from_bson(StringIO.new(body))
          tdr_data['tdr'] = body

          @current_logger.info p "Bunny ::: получили данные #{tdr_data}"

          if tdr_data.present?
            @current_logger.info p "Получен хеш tdr."
            delivery_tag = tdr_data['delivery_tag']
            
            # tdr = Tdr.new(tdr_data['tdr'])
            # tdr = Tdr.new(eval( tdr_data['tdr'] ))
            tdr = Tdr.new(tdr_data['tdr'])
            

            if tdr.present?
              @current_logger.info p "Новый tdr #{tdr} - imei - #{tdr.imei} ::: delivery_tag #{delivery_tag}"

              # используем данные редиса, которые публикуются scheduled_jobs
              # obd = $redis.get("svp:on_board_device:#{tdr.imei}")
              p "tdr.imei #{tdr.imei}"
              p obd = Db::OnBoardDevice.find_by_number("#{tdr.imei}")
              p "obd #{obd}"

              # obd_truck = $redis.get("svp:truck:#{eval(obd)['truck_id']}") if obd.present?
              p obd_truck = Db::Truck.find_by_id(obd.truck_id) if obd.present?
              p "obd_truck #{obd_truck}"

              # obd_truck_company = $redis.get("svp:company:#{eval(obd_truck)['company_id']}") if obd_truck.present?
              p obd_truck_company = Db::UserCard.find_by_id(obd_truck.user_card_id) if obd_truck.present?
              p "User Card #{obd_truck_company}"

              if obd.present? && obd_truck.present? && obd_truck_company.present?
                customer = Customer.new(obd_truck_company)   
                @current_logger.info p "customer #{customer}"
                @current_logger.info p "customer id #{customer.id}"
                @current_logger.info p "customer discount #{customer.discount}"
                
                # tariff_setting = $redis.get("svp:tariff_setting")
                tariff_setting = Db::TariffSetting.last

                # tariff_id = eval(tariff_setting) if tariff_setting.present?
                tariff_id = tariff_setting.id if tariff_setting.present?

                # current_tariff = eval($redis.get("svp:tariff:#{tariff_id}")) if tariff_id.present?
                current_tariff = Db::Tariff.find_by_id(tariff_id) if tariff_id.present?
                if current_tariff.present?  
                  # sum = eval(current_tariff['code']) 
                  sum = current_tariff.code
                end                       
                tdr.sum = sum
                @current_logger.info p "tdr #{tdr}"
                # send_tdr_data_to_rabbit(tdr, customer)  


                # отправка ack в канал
                @current_logger.info p "Отправка ack в RabbitMQ ::: delivery_tag: #{delivery_tag}"
                @ch.ack(delivery_tag)

                # p (Time.now.to_f - time_count)
                # if (((Time.now.to_f - time_count)*1000)%1000 == 0)
                #   message_count_recieve += 1
                #   sum_count += sum
                #   File.open(status_log_file, 'w') do |f|
                #     f.write "#{Time.now} TDR принято #{message_count_recieve}\n Средств начислено #{sum} (в #{(Time.now.to_i - time_count)} секунд) / #{sum_count} (всего)"
                #   end
                # end
                @current_logger.info p "Обработан tdr ::: delivery_tag #{delivery_tag} #{tdr} ::: sum #{sum} ::: #{tdr.full_info}"    
                
              end
            end        
          end

          # time2 = Time.now
          # @experiment_logger << (time2 - time1)
          # if @experiment_logger.size > 990
          #   m = @experiment_logger
          #   @current_logger.info p "Среднее время выполнения"
          #   p (m.inject(0){ |sum,el| sum + el }.to_f)/ m.size
          # end
        # rescue Exception => e
        #   puts "ERROR! #{e}"
        # end
      end
    end

    def night_time?
      false
    end

    def send_tdr_data_to_rabbit(tdr, customer)
      @current_logger.info p "Отправка tdr в RabbitMQ #{tdr} ::: sum: #{tdr.sum} ::: customer_id #{customer.id}"
      q    = @ch.queue($config['runner']['output_queue'], :durable => true)
      tdr_bson = BSON::Document.new(
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
        sum: tdr.sum,
        customer_id: customer.id
      )
      @ch.default_exchange.publish(tdr_bson.to_s, :routing_key => q.name)

      @current_logger.info p "Отправка tdr в RabbitMQ очередь 2 #{tdr} ::: sum: #{tdr.sum} ::: customer_id #{customer.id}"
      q2    = @ch.queue($config['runner']['output_queue2'], :durable => true)
      tdr_json = JSON.generate(
        # id машины
       {imei: tdr.imei, 
        road_id: tdr.full_info['road_id'], 
        lat0: tdr.full_info['lat0'], 
        lon0: tdr.full_info['lon0'], 
        time0: tdr.full_info['time0'], 
        lat1: tdr.full_info['lat1'], 
        lon1: tdr.full_info['lon1'], 
        time1: tdr.full_info['time1'], 
        path: tdr.full_info['path'],
        sum: tdr.sum}
      )
      @ch.default_exchange.publish(tdr_json.to_s, :routing_key => q2.name)
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
    def initialize(bson_doc)
      bson_doc = JSON.parse(bson_doc)
      @path = bson_doc['path']
      # здесь косяк с передачей imei
      @imei = bson_doc['imei'] # bson_doc['imei'][1].data
      @full_info = bson_doc
      @sum
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
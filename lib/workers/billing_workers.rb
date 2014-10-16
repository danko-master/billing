require 'bson'
require 'bunny'

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
        run
      rescue Interrupt => _
        @bunny.close
        @current_logger.info "NOTIFICATIONS: Stopped"
        exit(0)
      end 
    end

    def run   
      @current_logger.info p "Выполняем run, ждем tdr."  
      q    = @ch.queue($config['runner']['input_queue'], :durable => true) 
      q.subscribe(:block => true, :manual_ack => true) do |delivery_info, properties, body|
        time1 = Time.now

        tdr_data = Hash.new
        tdr_data['delivery_tag'] = delivery_info.delivery_tag
        tdr_data['tdr'] = body 
        @current_logger.info p "Bunny ::: получили данные #{tdr_data}"

        if tdr_data.present?
          @current_logger.info p "Получен хеш tdr."
          delivery_tag = tdr_data['delivery_tag']

          tdr = Tdr.new(eval( tdr_data['tdr'] ))

          if tdr.present?
            @current_logger.info p "Новый tdr #{tdr} ::: delivery_tag #{delivery_tag}"

            # используем данные редиса, которые публикуются scheduled_jobs
            obd = $redis.get("svp:on_board_device:#{tdr.imei}")
            # obd = Db::OnBoardDevice.find_by_number(tdr.imei)

            obd_truck = $redis.get("svp:truck:#{eval(obd)['truck_id']}") if obd.present?
            obd_truck_company = $redis.get("svp:company:#{eval(obd_truck)['company_id']}") if obd_truck.present?
            p "obd_truck_company #{obd_truck_company}"

            if obd.present? && obd_truck.present? && obd_truck_company.present?
              customer = Customer.new(eval(obd_truck_company))   
              p "customer #{customer}"
              p "customer #{customer.id}"
              p "customer #{customer.discount}"
              # current_tariff = Db::Tariff.find_by_id eval(Db::TariffSetting.last.code)
              
              tariff_setting = $redis.get("svp:tariff_setting")
              tariff_id = eval(tariff_setting) if tariff_setting.present?
              current_tariff = eval($redis.get("svp:tariff:#{tariff_id}")) if tariff_id.present?
              if current_tariff.present?  
                sum = eval(current_tariff['code']) 
              end                       
              tdr.sum = sum
              p tdr
              send_tdr_data_to_rabbit(tdr, customer)  


              # отправка ack в канал
              @ch.ack(delivery_tag)

              @current_logger.info p "Обработан tdr ::: delivery_tag #{delivery_tag} #{tdr} ::: sum #{sum} ::: #{tdr.full_info}"    
            end
          end        
        end

        time2 = Time.now
        @experiment_logger << (time2 - time1)
        if @experiment_logger.size > 990
          m = @experiment_logger
          @current_logger.info p "Среднее время выполнения"
          p (m.inject(0){ |sum,el| sum + el }.to_f)/ m.size
        end
      end
    end

    def night_time?
      false
    end

    def send_tdr_data_to_rabbit(tdr, customer)
      @current_logger.info p "Отправка tdr в RabbitMQ #{tdr} ::: sum: #{tdr.sum} ::: customer_id #{customer.id}"
      q    = @ch.queue($config['runner']['output_queue'])

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
    end
  end

  class Customer
    def initialize(hash)
      @id = hash['id']
      @discount = hash['discount']
    end

    def id
      @id 
    end

    def discount
      @discount
    end
  end

  class Tdr
    def initialize(hash)
      @path = hash['path']
      @imei = hash['imei'].to_s
      @full_info = hash
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
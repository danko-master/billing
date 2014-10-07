require 'bson'
require 'bunny'

module BillingWorkers
  class Calc
    include Sidekiq::Worker
    sidekiq_options queue: :billing

    def perform(num_inst)
      @bunny = Bunny.new
      @flag = true
      loop do
        if @flag == true
          @flag = false
          @current_logger = Logger.new("#{File.dirname(__FILE__)}/../../log/sidekiq_#{ENV['APP_ENV']}_inst_#{num_inst}.log")
          @current_logger.info { "NOTIFICATIONS: Started" }        
          @bunny.start
          @ch   = @bunny.create_channel
          run
          @bunny.close
          @flag = true
        end
      end
    end

    def run     
      tdr_data = get_tdr_data_from_rabbit
      if tdr_data.present?
        delivery_tag = tdr_data['delivery_tag']

        tdr = Tdr.new(eval( tdr_data['tdr'] ))
        if tdr.present?
          obd = Db::OnBoardDevice.find_by_number(tdr.imei)
          if obd.present? && obd.truck.present? && obd.truck.company.present?
            customer = obd.truck.company   
            current_tariff = Db::Tariff.find_by_id eval(Db::TariffSetting.last.code)           
            sum = eval(current_tariff.code)                         
            tdr.sum = sum
            p tdr
            send_tdr_data_to_rabbit(tdr, customer)  

            # отправка ack в канал
            @ch.ack(delivery_tag)

            @current_logger.info p "Обработан tdr #{tdr} ::: sum #{sum} ::: #{tdr.full_info}"    
          end
        end
        
      end
    end

    def night_time?
      false
    end

    # информация из TDR-тарифов
    def get_tdr_data_from_rabbit
      q    = @ch.queue($config['runner']['input_queue'])   
      tdr_data = nil
      q.subscribe(:manual_ack => true) do |delivery_info, properties, body|
        tdr_data = Hash.new
        
        tdr_data['delivery_tag'] = delivery_info.delivery_tag
        tdr_data['tdr'] = body

        @current_logger.info p "Bunny ::: получили данные #{tdr_data}"
      end     

      # @current_logger.info p "Bunny ::: recieve data #{tdr_data}"
      tdr_data
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
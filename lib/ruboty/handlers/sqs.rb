require 'ruboty'
require 'aws-sdk'

class Ruboty::Handlers::SQS < Ruboty::Handlers::Base
  env :SQS_ROOM, 'Default room for receive message'
  env :SQS_URL, 'URL of SQS Endpoint'
  env :AWS_REGION, 'Region of AWS', optional: true

  def initialize(*args)
    super

    polling
  end

  def send_message(message)
    channel = (message[:message_attributes] || {})['channel']
    channel = channel ? channel[:string_value] : ENV['SQS_ROOM']

    robot.say(
      body: message[:body],
      to: channel
    )
  end

  private

  def sqs
    @sqs = ::Aws::SQS::Client.new()
  end

  def polling
    @polling_thread ||= begin
      th = Thread.new(self, sqs) do |robot, sqs|
        loop do
          begin
            resp = sqs.receive_message(
              queue_url: ENV['SQS_URL'],
              attribute_names: %w(All),
              message_attribute_names: [],
              max_number_of_messages: 3,
              wait_time_seconds: 10
            )

            messages = resp[:messages]

            messages.each do |message|
              robot.send_message(message)
            end

            sqs.delete_message_batch(
              queue_url: ENV['SQS_URL'],
              entries: messages.map { |m| { id: m[:message_id], receipt_handle: m[:receipt_handle] } }
            ) unless messages.empty?
          rescue Aws::SQS::Errors::ServiceError => e
            p e
          rescue => e
            p e
          end
        end
      end
      th.run
      th
    end
  end
end

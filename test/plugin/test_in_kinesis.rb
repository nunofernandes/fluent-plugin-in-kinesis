require 'helper'

class KinesisOutputTest < Test::Unit::TestCase
  include Fluent::Test::Helpers

  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    aws_key_id test_key_id
    aws_sec_key test_sec_key
    stream_name test_stream
    region ap-northeast-1
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::S3Input).configure(conf)
  end

  class ConfigTest < self
    def test_default
      d = create_driver
      extractor = d.instance.instance_variable_get(:@extractor)
      actual = {
        aws_key_id: d.instance.aws_key_id,
        aws_sec_key: d.instance.aws_sec_key,
        stream_name: d.instance.stream_name,
        region: d.instance.region,
        sqs_queue_name: d.instance.sqs.queue_name,
        extractor_ext: extractor.ext,
        extractor_content_type: extractor.content_type
      }
      expected = {
        aws_key_id: "test_key_id",
        aws_sec_key: "test_sec_key",
        s3_bucket: "test_bucket",
        s3_region: "us-east-1",
        sqs_queue_name: "test_queue",
        extractor_ext: "gz",
        extractor_content_type: "application/x-gzip"
      }
      assert_equal(expected, actual)
    end

    def test_empty
      assert_raise(Fluent::ConfigError) do
        create_driver("")
      end
    end
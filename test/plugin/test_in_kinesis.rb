require 'helper'

class KinesisInputTest < Test::Unit::TestCase
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
      }
      expected = {
        aws_key_id: "test_key_id",
        aws_sec_key: "test_sec_key",
        stream_name: "test_stream",
        region: "ap-northeast-1",
      }
      assert_equal(expected, actual)
    end

    def test_empty
      assert_raise(Fluent::ConfigError) do
        create_driver("")
      end
    end
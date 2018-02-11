require 'helper'
require 'fluent/plugin/in_kinesis'

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
    state_dir_path dir/path
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Input
      .new(FluentPluginKinesis::InputFilter).configure(conf)
  end
end

  def create_mock_client
    client = mock(Object.new)
    stub(Aws::Kinesis::Client).new(anything) { client }
    return client
  end

  def test_configure
    d = create_driver
    assert_equal 'test_key_id', d.instance.aws_key_id
    assert_equal 'test_sec_key', d.instance.aws_sec_key
    assert_equal 'test_stream', d.instance.stream_name
    assert_equal 'test_region', d.instance.region
    assert_equal 'state_dir_path', d.instance.state_dir_path
  end

  def test_configure_with_credentials
    d = create_driver(<<-EOS)
      profile default
      credential_path /home/foo/.aws./credentials
      stream_name test_stream
      region ap-northeast-1
      state_dir_path /var/log/in_kinesis
    EOS

    assert_equal 'default', d.instance.profile
    assert_equal '/home/foo/.aws/credentials', d.instance.credential_path
    assert_equal 'test_stream', d.instance.stream_name
    assert_equal 'ap-northeast-1', d.instance.region
    assert_equal '/var/log/in_kinesis', d.instance.state_dir_path
  end

  def test_load_client
    client = stub(Object.new)
    client.get_records { {} }

    stub(Aws::Kinesis::Client).new do |options|
      assert_equal("test_key_id", options[:access_key_id])
      assert_equal("test_sec_key", options[:secret_access_key])
      assert_equal("ap-northeast-1", options[:region])
      assert_equal("test_stream", options[:stream_name])
      assert_equal("/var/log/in_kinesis", options[:state_dir_path])
      client
    end

    d = create_driver
    d.run(default_tag: "test")
  end

  def test_load_client_with_credentials
    client = stub(Object.new)
    client.get_records { {} }

    stub(Aws::Kinesis::Client).new do |options|
      assert_equal(nil, options[:access_key_id])
      assert_equal(nil, options[:secret_access_key])
      assert_equal("ap-northeast-1", options[:region])

      credentials = options[:credentials]
      assert_equal("default", credentials.profile_name)
      assert_equal("/home/foo/.aws/credentials", credentials.path)

      client
    end

    d = create_driver(<<-EOS)
      profile default
      credentials_path /home/foo/.aws/credentials
      stream_name test_stream
      region ap-northeast-1
      state_dir_path /var/log/in_kinesis
    EOS

    d.run(default_tag: "test")
  end
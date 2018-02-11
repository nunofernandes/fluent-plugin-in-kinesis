require "bundler"
Bundler::GemHelper.install_tasks

require "rake/testtask"
Rake::TestTask.new(:test) do |test|
    test.libs.push("lib", "test")
    test.test_files = FileList["test/**/test_*.rb"]
    test.verbose = true
    test.warning = true
end

task :default => [:test]

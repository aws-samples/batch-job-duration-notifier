from projen.awscdk import AwsCdkPythonApp

project = AwsCdkPythonApp(
    author_email="awsnick@amazon.com",
    author_name="Nick Dobson",
    cdk_version="2.140.0",
    module_name="batch_monitor",
    name="batch-monitor",
    version="0.1.0",
)
project.add_git_ignore(".DS_Store")
project.synth()
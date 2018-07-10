# plan-test
A test about Python and other technologies

# To install 
sudo apt install python-dev libmysqlclient-dev
sudo pip install mysqlclient

export PYTHONPATH=/opt/repos/plan-test/

python -m unittest test_extractor.TestExtractor

# Prepare repo.
sudo mkdir /opt/repos
chown ubuntu:ubuntu repos/
cd /opt/repos/
git clone https://github.com/nordev/plan-test.git

# Execute test with

sh /opt/repos/plan-test/test/execute_test.sh

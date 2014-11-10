<?php

namespace PerfectWeb\Cassandra\PDOCassandra;

use Doctrine\DBAL\Driver as BaseDriver;
use Doctrine\DBAL\Driver\PDOMySql\Driver as MySqlDriver;
use PerfectWeb\Cassandra\Platform\CassandraPlatform;

class Driver extends MySqlDriver implements BaseDriver
{

	/**
	 * {@inheritdoc}
	 */
	public function connect(array $params, $username = null, $password = null, array $driverOptions = array())
	{

        $conn = new Connection($this->_constructPdoDsn($params), $username, $password, $driverOptions);

		if (isset($params['dbname'])) {
			$conn->exec('USE '.$params['dbname']);
		}

		return $conn;

	}

	/**
	 * Constructs the MySql PDO DSN.
	 *
	 * @param array $params
	 *
	 * @return string The DSN.
	 */
	private function _constructPdoDsn(array $params)
	{

		$dsn = 'cassandra:';
		if (isset($params['host']) && $params['host'] != '') {
			$dsn .= 'host='.$params['host'].';';
		}

		foreach ($params['servers'] as $server) {
			$dsn .= 'host='.$server['host'].';';
			$dsn .= 'port='.(empty($server['port']) ? 9160 : $server['port']).',';
		}

		$dsn = trim($dsn, ',');

		if (isset($params['cqlversion'])) {
			$dsn .= ';cqlversion='.$params['cqlversion'];
		}

		return $dsn;

	}

	/**
	 * {@inheritdoc}
	 */
	public function getName()
	{
		return 'cassandra';
	}

	/**
	 * {@inheritdoc}
	 */
	public function getDatabasePlatform()
	{
		return new CassandraPlatform();
	}

}
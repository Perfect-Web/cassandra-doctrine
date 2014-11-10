<?php

namespace PerfectWeb\Cassandra\PDOCassandra;


class Connection {

	function beginTransaction()
	{
		$database->beginBatch();
	}

	function commit()
	{
		return false;
	}

	function rollBack()
	{
		return false;
	}

    function lastInsertId()
	{
		throw new \RuntimeException('method not implemented');
	}

    function setAttribute()
	{
		return false;
	}

}
<?php

namespace PerfectWeb\Cassandra\PDOCassandra;

use Cassandra\Connection as CassandraConnection;
use Doctrine\DBAL\Connection as DoctrineConnection;

class Connection extends DoctrineConnection
{

    /**
     * @param array $nodes
     * @param \Doctrine\DBAL\Driver $keyspace
     */
    function __construct($nodes, $keyspace)
    {
        return new CassandraConnection($nodes, $keyspace);
    }

    public function connect()
    {
        return true;
    }

    public function prepare($statement)
    {

    }

}
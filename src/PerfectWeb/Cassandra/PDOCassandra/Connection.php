<?php

namespace PerfectWeb\Cassandra\PDOCassandra;

use Cassandra\Connection as CassandraConnection;
use Doctrine\DBAL\Connection as DoctrineConnection;
use PerfectWeb\Cassandra\Platform\CassandraPlatform;
use Doctrine\DBAL\Cache\QueryCacheProfile;
use Doctrine\DBAL\SQLParserUtils;

class Connection extends DoctrineConnection
{

    /**
     * @param array $nodes
     * @param \Doctrine\DBAL\Driver $keyspace
     */
    function __construct($nodes, $keyspace)
    {
        $this->_conn = new CassandraConnection($nodes, $keyspace);
        return $this->_conn;
    }

    /**
     * {@inheritdoc}
     */
    public function prepare($statement)
    {
        return $this->_conn->prepare($statement);
    }

    public function getDatabasePlatform()
    {
        return new CassandraPlatform();
    }

}
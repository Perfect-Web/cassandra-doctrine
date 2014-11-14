<?php

namespace PerfectWeb\Cassandra\PDOCassandra;

use Cassandra\Connection as CassandraConnection;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Cache\QueryCacheProfile;
use Doctrine\DBAL\SQLParserUtils;
use Doctrine\DBAL\DBALException;

class ConnectionWrapper extends Connection
{

    /**
     * {@inheritdoc}
     */
    public function executeQuery($query, array $params = array(), $types = array(), QueryCacheProfile $qcp = null)
    {
        if ($qcp !== null) {
            return $this->executeCacheQuery($query, $params, $types, $qcp);
        }

        $this->connect();

        $logger = $this->_config->getSQLLogger();
        if ($logger) {
            $logger->startQuery($query, $params, $types);
        }

        try {

            list($query, $params, $types) = SQLParserUtils::expandListParameters($query, $params, $types);

            $preparedData = $this->_conn->prepare($query);

            $strictValues = \Cassandra\Request\Request::strictTypeValues($params, $preparedData['metadata']['columns']);
            $stmt = $this->_conn->executeSync($preparedData['id'], $strictValues);
            $stmt->setMetadata($preparedData['result_metadata']);

        }
        catch (\Exception $ex) {
            throw DBALException::driverExceptionDuringQuery($this->_driver, $ex, $query, $this->resolveParams($params, $types));
        }

        if ($logger) {
            $logger->stopQuery();
        }

        return $stmt;

    }

}
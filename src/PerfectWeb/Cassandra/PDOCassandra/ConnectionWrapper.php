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
            if ($params) {
                list($query, $params, $types) = SQLParserUtils::expandListParameters($query, $params, $types);

                $stmt = $this->_conn->prepare($query);

                if ($types) {
                    $this->_bindTypedValues($stmt, $params, $types);
                    $stmt->execute();
                } else {
                    $stmt->execute($params);
                }
            } else {
                $stmt = $this->_conn->query($query);
            }
        } catch (\Exception $ex) {
            throw DBALException::driverExceptionDuringQuery($this->_driver, $ex, $query, $this->resolveParams($params, $types));
        }

        if ($logger) {
            $logger->stopQuery();
        }

        return $stmt;

    }

}
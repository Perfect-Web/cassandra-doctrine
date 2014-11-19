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
    public function executeUpdate($query, array $params = array(), array $types = array())
    {
        return call_user_func_array([$this, 'executeQuery'], func_get_args());
    }

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

    /**
     * {@inheritdoc}
     */
    public function quote($input, $type = null)
    {
        $this->connect();
        return "'".addslashes($input)."'";
    }

    /**
     * {@inheritdoc}
     */
    public function exec($statement)
    {

        $this->connect();

        $logger = $this->_config->getSQLLogger();
        if ($logger) {
            $logger->startQuery($statement);
        }

        try {
            $this->executeQuery($statement);
        } catch (\Exception $ex) {
            throw DBALException::driverExceptionDuringQuery($this->_driver, $ex, $statement);
        }

        if ($logger) {
            $logger->stopQuery();
        }

        return 1;

    }

    /**
     * {@inheritdoc}
     */
    public function beginTransaction()
    {
        return false;
    }

    /**
     * {@inheritdoc}
     */
    public function prepare($statement)
    {
        $this->connect();

        try {
            $stmt = new Statement($statement, $this);
        } catch (\Exception $ex) {
            throw DBALException::driverExceptionDuringQuery($this->_driver, $ex, $statement);
        }

        return $stmt;
    }

}
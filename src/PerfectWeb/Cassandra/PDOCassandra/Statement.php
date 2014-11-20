<?php
/**
 * Created by PhpStorm.
 * User: Alin Jurj
 * Date: 11/19/14
 * Time: 10:12 PM
 */

namespace PerfectWeb\Cassandra\PDOCassandra;

use Doctrine\DBAL\Driver\Mysqli\MysqliStatement;
use Cassandra\Request\Request;
use Cassandra\Type;

class Statement extends MysqliStatement
{

    private $_stmtPrepared = null;

    /**
     * {@inheritdoc}
     */
    public function __construct($conn, $prepareString)
    {
        $this->_conn = $conn;
        $this->_stmt = $prepareString;
        $this->_stmtPrepared = $this->_conn->prepare($prepareString);
    }

    /**
     * {@inheritdoc}
     */
    public function closeCursor()
    {
        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function setFetchMode($fetchMode, $arg2 = null, $arg3 = null)
    {
        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function execute($params = null)
    {
        return $this->_conn->executeSync(
            $this->_stmtPrepared['id'],
            Request::strictTypeValues(
                is_null($params) ? $this->_bindedValues : $params,
                $this->_stmtPrepared['metadata']['columns']
            )
        );
    }

    /**
     * {@inheritdoc}
     */
    public function bindValue($param, $value, $type = null)
    {

        // we need to decrement the param because of the persister that starts the index with 1 rather than 0
        if (is_numeric($param)) {
            $param--;
        }

        $this->_values[$param] = $value;
        $this->_bindedValues[$param] =& $this->_values[$param];
        $this->types[$param - 1] = $type;

        return true;
    }

}
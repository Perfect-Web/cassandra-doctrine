<?php
/**
 * Created by PhpStorm.
 * User: Alin Jurj
 * Date: 11/19/14
 * Time: 10:12 PM
 */

namespace PerfectWeb\Cassandra\PDOCassandra;

use Doctrine\DBAL\Statement as DoctrineStatement;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Driver\Mysqli\MysqliStatement;

class Statement extends MysqliStatement
{

    /**
     * @param $conn
     * @param string  $prepareString
     *
     * @throws \Doctrine\DBAL\Driver\Mysqli\MysqliException
     */
    public function __construct($conn, $prepareString)
    {
        return call_user_func_array([$this, '__construct'], func_get_args());
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
    public function execute($params = null)
    {
        if (null !== $this->_bindedValues) {
            if (null !== $params) {
                if ( ! $this->_bindValues($params)) {
                    throw new MysqliException($this->_stmt->error, $this->_stmt->errno);
                }
            } else {
                if (!call_user_func_array(array($this->_stmt, 'bind_param'), array($this->types) + $this->_bindedValues)) {
                    throw new MysqliException($this->_stmt->error, $this->_stmt->sqlstate, $this->_stmt->errno);
                }
            }
        }

        if ( ! $this->_stmt->execute()) {
            throw new MysqliException($this->_stmt->error, $this->_stmt->sqlstate, $this->_stmt->errno);
        }

        if (null === $this->_columnNames) {
            $meta = $this->_stmt->result_metadata();
            if (false !== $meta) {
                // We have a result.
                $this->_stmt->store_result();

                $columnNames = array();
                foreach ($meta->fetch_fields() as $col) {
                    $columnNames[] = $col->name;
                }
                $meta->free();

                $this->_columnNames = $columnNames;
                $this->_rowBindedValues = array_fill(0, count($columnNames), NULL);

                $refs = array();
                foreach ($this->_rowBindedValues as $key => &$value) {
                    $refs[$key] =& $value;
                }

                if (!call_user_func_array(array($this->_stmt, 'bind_result'), $refs)) {
                    throw new MysqliException($this->_stmt->error, $this->_stmt->sqlstate, $this->_stmt->errno);
                }
            } else {
                $this->_columnNames = false;
            }
        }

        return true;
    }

}
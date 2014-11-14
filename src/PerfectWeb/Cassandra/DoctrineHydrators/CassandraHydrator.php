<?php

namespace PerfectWeb\Cassandra\DoctrineHydrators;

use Doctrine\ORM\Internal\Hydration\ObjectHydrator;
use Doctrine\ORM\UnitOfWork;


class CassandraHydrator extends ObjectHydrator
{

    const CassandraHydrator = 'CassandraHydrator';

    /**
     * {@inheritdoc}
     */
    protected function hydrateAllData()
    {

        $result = array();

        foreach ($this->_stmt->fetchAll() as $row) {
            $this->hydrateRowData($row, $result);
        }

        return $result;

    }

    /**
     * {@inheritdoc}
     */
    protected function cleanup()
    {

        $eagerLoad = (isset($this->_hints[UnitOfWork::HINT_DEFEREAGERLOAD])) && $this->_hints[UnitOfWork::HINT_DEFEREAGERLOAD] == true;

        $this->_stmt          = null;
        $this->_rsm           = null;
        $this->_cache         = array();
        $this->_metadataCache = array();

        if ($eagerLoad) {
            $this->_em->getUnitOfWork()->triggerEagerLoads();
        }

    }

}
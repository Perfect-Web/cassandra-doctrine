<?php

namespace PerfectWeb\Cassandra\ORM;

use Doctrine\ORM\Query\ResultSetMappingBuilder;
use Doctrine\DBAL\Types\Type;
use Doctrine\ORM\Query\Expr\Func;
use Doctrine\ORM\Query\Expr\Comparison;
use Doctrine\ORM\AbstractQuery;
use PerfectWeb\Cassandra\ORM\Query\Expr\Andx;

trait EntityRepositoryTrait
{

	protected $_defaultHydrator = AbstractQuery::HYDRATE_OBJECT;

	/*protected $_hydrator = null;*/

	/**
	 * @param array $criteria
	 * @param array $orderBy
	 * @param null  $limit
	 * @param null  $offset
	 * @param bool  $innerBubble
	 *
	 * @return array
	 */
	public function findBy(array $criteria, array $orderBy = null, $limit = null, $offset = null, $innerBubble = false)
	{

		$rsm = new ResultSetMappingBuilder($this->getEntityManager());
		$rsm->addRootEntityFromClassMetadata($this->getEntityName(), 'e');
        $qb = $this->getEntityManager()->createQueryBuilder();
        $platform = $this->getEntityManager()->getConnection()->getDatabasePlatform();
		$result = $params = [];

        foreach ($criteria as $column => &$value) {

	        if (!is_object($value)) {
                $params[$column] = $value;
            }

	        if (!$innerBubble && ($value instanceof Func) && strpos($value->getName(), ' IN') !== false) {
		        $lengths = 0;
		        foreach ($value->getArguments() as $in) {
			        $lengths += strlen($in);
			        $ids[$in] = $in;
			        if ($lengths > 65000) {
				        $result = array_merge(
					        $result,
					        $this->findBy(
					            array_merge($criteria, [$column => $qb->expr()->in($column, $ids)]),
				                $orderBy,
				                $limit,
				                $offset,
					            true
			                )
				        );
				        $lengths = 0;
				        $ids = [];
			        }
		        }
		        $result = array_merge(
			        $result,
					$this->findBy(
					    array_merge($criteria, [$column => $qb->expr()->in($column, $ids)]),
				        $orderBy,
				        $limit,
				        $offset,
					    true
		            )
			    );
	        }

	        // fields not existing in the entity are ignored
	        elseif ($this->getClassMetadata()->hasField($column)) {

                if (!$innerBubble  &&
	                !is_subclass_of($value, 'Doctrine\ORM\Query\Expr\Base') &&
	                !($value instanceof Comparison) &&
	                !($value instanceof Func)
                )
                {
		            $mapping = $this->getClassMetadata()->getFieldMapping($column);
		            $value = Type::getType($mapping['type'])->convertToDatabaseValue($value, $platform);
		            $exp[] = $qb->expr()->eq($mapping['columnName'], ":".$mapping['fieldName']);
	            }
                else $exp[] = $value;

	        }

        }

		if (!empty($exp)) { // it may be empty if recurrence is taking place

	        $cql = $qb->where(call_user_func_array(function(){return new Andx(func_get_args());}, $exp))
	                  ->select($this->getSelectColumns($criteria) . ' from '.$this->getClassMetadata()->getTableName())
	                  ->setMaxResults($limit);

			$query = $this->getEntityManager()
						  ->createNativeQuery($cql.' allow filtering', $rsm)
						  ->setParameters($params);

			$hydrator = !is_null($this->_hydrator) ? $this->_hydrator : $this->_defaultHydrator;

			$result = array_merge(
				$result,
				($limit == 1) ? $query->getSingleResult() : $query->getResult($hydrator)
			);

		}

		return $result;
	}

	/**
	 * @param array $criteria
	 * @param array $orderBy
	 *
	 * @return array|null|object
	 */
	public function findOneBy(array $criteria, array $orderBy = null)
	{
		return $this->findBy($criteria, $orderBy, 1);
	}

	/**
	 * @param array $criteria
	 *
	 * @return string
	 */
	public function getSelectColumns(array $criteria = [])
	{
		return '*';
	}

}
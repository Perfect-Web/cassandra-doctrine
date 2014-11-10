<?php

namespace PerfectWeb\Cassandra\ORM\Query\Expr;

use Doctrine\ORM\Query\Expr\Andx as DoctrineExprAndx;

class Andx extends DoctrineExprAndx
{
    /**
     * @var string
     */
    protected $preSeparator = '';

    /**
     * @var string
     */
    protected $postSeparator = '';

	public function __construct($args = array())
	{
		$this->allowedClasses[] = 'PerfectWeb\Cassandra\ORM\Query\Expr\Andx';
		parent::__construct($args);
	}

}
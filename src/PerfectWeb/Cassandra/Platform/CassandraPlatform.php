<?php

namespace PerfectWeb\Cassandra\Platform;

use Doctrine\DBAL\Platforms\MySqlPlatform;


class CassandraPlatform extends MySqlPlatform
{

	/**
	 * {@inheritDoc}
	 */
	public function getName()
	{
		return 'cassandra';
	}

}

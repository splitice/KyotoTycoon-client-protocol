<?php
namespace Splitice\Kyoto;
use Iterator, ArrayAccess, DomainException, OutOfBoundsException, RuntimeException, LogicException;
/**
 * Thrown when the connection to the KyotoTycoon cannot be established.
 */
class ConnectionException extends RuntimeException
{
    function __construct( $uri, $msg )
    {
        parent::__construct( "Couldn't connect to KyotoTycoon server {$uri}. {$msg}", 1 );
    }
}
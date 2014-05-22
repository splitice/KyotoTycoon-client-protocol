<?php
namespace Splitice\Kyoto;
use Iterator, ArrayAccess, DomainException, OutOfBoundsException, RuntimeException, LogicException;
/**
 * Created by PhpStorm.
 * User: splitice
 * Date: 5/22/14
 * Time: 9:24 PM
 */
/**
 * Throw if the protocol isn't well implemented for an operation.
 */
class ProtocolException extends DomainException
{
    function __construct( $uri )
    {
        parent::__construct( "Bad protocol communication with the KyotoTycoon server {$uri}.", 3 );
    }
}
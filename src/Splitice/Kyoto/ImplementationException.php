<?php
namespace Splitice\Kyoto;
use Iterator, ArrayAccess, DomainException, OutOfBoundsException, RuntimeException, LogicException;
/**
 * Created by PhpStorm.
 * User: splitice
 * Date: 5/22/14
 * Time: 9:24 PM
 */
class ImplementationException extends LogicException
{
    function __construct( $uri )
    {
        parent::__construct( "Unimplented procedure on the selected database storage type with the KyotoTycoon server {$uri}.", 4 );
    }
}
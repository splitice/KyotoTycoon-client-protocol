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
 * Thrown when an operation is asked about a record that didn't respect all the needs.
 * The processing is done but the result is not fulfill the application logic.
 */
class InconsistencyException extends OutOfBoundsException
{
    function __construct( $uri, $msg )
    {
        parent::__construct( "(Un)existing record was detected on server {$uri}. {$msg}", 2 );
    }
}
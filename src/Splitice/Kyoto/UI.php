<?php
namespace Splitice\Kyoto;
use Iterator, ArrayAccess, DomainException, OutOfBoundsException, RuntimeException, LogicException;

/**
 * Return an UI object ready to send command to a KyotoTycoon server.
 * Params:
 *	 string $uri = The URI of the KyotoTycoon server.
 * Return:
 *	 UI = The User Interface object.
 * Set the connection parameters:
 * ----
 * $kt = UI(); // Default parameters is: localhost:1978 Corresponding to the first database loaded by the server.
 * $kt = UI('http://kt.local:1979/user.kch');
 * ----
 * Set and get value of the records:
 * ----
 * // Using ArrayAccess
 * $kt['japan'] = 'tokyo';
 * var_dump( $kt['japan']);
 * // Using method
 * $kt->set('france','paris');
 * var_dump( $kt->get('france') );
 * // Using magic method
 * $kt->coruscant('coruscant');
 * var_dump( $kt->coruscant );
 * ----
 * Set and get the expiration time of a record.
 * ----
 * $kt->set('a','ananas',2);
 * var_dump( $kt->gxt('a') );
 * ----
 * Browsing keys
 * ----
 * // Keys begins with a prefix
 * foreach( $kt->prefix('prefix_') as $key )
 *   var_dump( $key );
 * // Keys matchs a regular expression
 * foreach( $kt->search('.*_match_.*') as $key )
 *   var_dump( $key );
 * ----
 * Browsing records
 * // Keys begins with a prefix
 * foreach( $kt->begin('prefix_') as $key => $value )
 *   var_dump( $key, $value );
 * // Keys matchs a regular expression
 * foreach( $kt->regex('.*_match_.*') as $key => $value )
 *   var_dump( $key, $value );
 * // All records
 * foreach( $kt->forward() as $key => $value )
 *   var_dump( $key, $value );
 * // All records starting at a key
 * foreach( $kt->forward('first') as $key => $value )
 *   var_dump( $key, $value );
 * // All records in reverse order
 * foreach( $kt->backward() as $key => $value )
 *   var_dump( $key, $value );
 * // All records in reverse order starting at a key
 * foreach( $kt->backward('last') as $key => $value )
 *   var_dump( $key, $value );
 * ----
 */

/**
 * Fluent and quick user interface (UI) for the KyotoTycoon API.
 *
 */
final class UI implements Iterator, ArrayAccess
{
    // {{{ ---properties
    // The API object used to send command.
    private $api = null;
    function api() { return $this->api; }

    // Indicate if OutOfBoundsException should be throw instead of returning null.
    private $outofbound = true;

    // Indicate if RuntimeException should be throw instead of returning false.
    private $runtime = true;

    // Used to store the prefixe before initiate the process of browsing the keys.
    private $prefix = null;

    // Used to store the regex before intitiate the process of browsing the keys.
    private $regex = null;

    private $just_key = false;

    // Indicate the maximum number of keys returned by match_prefix and match_regex operations.
    private $max = null;

    // Used to store the retreived number of records founds with match_prefix and match_regex operations.
    private $num = null;

    // Used to store all the keys returned by match_prefix and match_regex operations.
    private $keys = null;

    // Used to store temporally the key and the value of a retrieved records during any browse operations.
    private $record = null;

    // Indicate the direction of the browsing operation.
    private $backward = null;

    // Set to store the current used cursor (CUR).
    private $cursor = null;

    // Indiquate the first key of a browsing operation.
    private $startkey = null;

    // Maintain a list of all used Kyoto Tycoon cursor (CUR).
    static $cursors = array();

    // }}}
    // {{{ __construct(), __clone()

    function __construct( $uri = 'http://localhost:1978' )
    {
        assert('is_array(parse_url($uri))');
        $this->api = new API( $uri );
    }

    function __destruct()
    {
        if( ! is_null($this->cursor) )
        {
            assert('is_integer($this->cursor)');
            unset(self::$cursors[$this->cursor]);
        }
    }

    function __clone()
    {
        $this->prefix = null;
        $this->regex = null;
        $this->just_key = false;
        $this->max = null;
        $this->num = null;
        $this->cursor = null;
        $this->keys = null;
        $this->record = null;
        $this->backward = null;
        $this->startkey = null;
    }

    function clear(){
        $this->api->clear();
        return $this;
    }

    // }}}
    // {{{ __get(), __isset(), __unset(), __call()

    function __get( $property )
    {
        assert('is_string($property)');
        switch( $property )
        {
            case 'api':
                return $this->api;
            case 'outofbound_throw_exception':
                $this->outofbound = true;
                return $this;
            case 'outofbound_return_null':
                $this->outofbound = false;
                return $this;
            case 'runtime_throw_exception':
                $this->runtime = true;
                return $this;
            case 'runtime_return_false':
                $this->runtime = false;
                return $this;
            default:
                try { return $this->api->get($property,$xt); }
                catch( OutOfBoundsException $e ) { if( $this->outofbound ) throw $e; else return null; }
                catch( RuntimeException $e ) { if( $this->runtime ) throw $e; else return false; }
        }
    }

    function __isset( $key )
    {
        assert('is_string($key)');
        try { return is_string($this->api->get($key,$xt)); }
        catch( OutOfBoundsException $e ) { if( $this->outofbound ) throw $e; else return false; }
        catch( RuntimeException $e ) { if( $this->runtime ) throw $e; else return false; }
    }

    function __unset( $key )
    {
        assert('is_string($key)');
        $this->del($key);
    }

    function __call( $method, $args )
    {
        assert('is_string($method)');
        assert('is_scalar($args[0])');
        return $this->set($method, (string)$args[0]);
    }

    // }}}
    // {{{ get(), gxt(), set(), inc(), cat(), add(), rep(), del(), cas()

    /**
     * Retrieve the value of a record.
     * Params:
     *	 string $key = The key of the record.
     *	 (out) integer $xt = The absolute expiration time.
     *	 (out) null $xt = There is no expiration time.
     * Return:
     *	 string = The value of the record.
     *	 null = If the record do not exists.
     *	 false = If an error ocurred.
     */
    function get( $key, &$xt = null )
    {
        assert('is_string($key)');
        try { return $this->api->get($key,$xt); }
        catch( OutOfBoundsException $e ) { if( $this->outofbound ) throw $e; else return null; }
        catch( RuntimeException $e ) { if( $this->runtime ) throw $e; else return false; }
    }

    /**
     * Retrieve the value of a record.
     * Params:
     *	 string $key = The key of the record.
     *	 (out) integer $xt = The absolute expiration time.
     *	 (out) null $vsiz = There is no size information.
     *	 (out) null $xt = There is no expiration time.
     * Return:
     *	 string = The value of the record.
     *	 null = If the record do not exists.
     *	 false = If an error ocurred.
     */
    function check( $key, &$vsiz = null, &$xt = null )
    {
        assert('is_string($key)');
        try { return $this->api->check($key,$xt,$vsiz); }
        catch( OutOfBoundsException $e ) { if( $this->outofbound ) throw $e; else return null; }
        catch( RuntimeException $e ) { if( $this->runtime ) throw $e; else return false; }
    }

    /**
     * Retrieve the expiration time of a record.
     * Params:
     *	 string $key = The key of the record.
     * Return:
     *	 string = The value of the expiration time.
     *	 null = If the record do not exists.
     *	 false = If an error ocurred.
     */
    function gxt( $key )
    {
        assert('is_string($key)');
        $xt = null;
        try { $this->api->get($key,$xt); return $xt; }
        catch( OutOfBoundsException $e ) { if( $this->outofbound ) throw $e; else return null; }
        catch( RuntimeException $e ) { if( $this->runtime ) throw $e; else return false; }
    }

    /**
     * Set the value of a record.
     * Params:
     *	 string $key = The key of the record.
     *	 string $value = The value of the record.
     *	 numeric $xt = The expiration time from now in seconds. If it is negative, the absolute value is treated as the epoch time.
     *	 null $xt = No expiration time is specified.
     * Return:
     *   true = If success.
     *   false = If an error ocurred.
     */
    function set( $key, $value, $xt = null )
    {
        assert('is_string($key)');
        assert('is_string($value)');
        assert('is_null($xt) or is_numeric($xt)');
        try { $this->api->set($key,$value,$xt); return true; }
        catch( RuntimeException $e ) { if( $this->runtime ) throw $e; else return false; }
    }

    function inc( $key, $num = 1, $xt = null )
    {
        assert('is_string($key)');
        assert('is_numeric($num)');
        assert('is_null($xt) or is_numeric($xt)');
        try
        {
            if( is_integer($num) or (string)(int)$num===$num )
                return $this->api->increment( $key, $num, $xt );
            else
                return $this->api->increment_double( $key, $num, $xt );
        }
        catch( OutOfBoundsException $e ) { if( $this->outofbound ) throw $e; else return null; }
    }

    /**
     * Append the value to a record.
     * Params:
     *	 string $key = The key of the record.
     *	 string $value = The value of the record.
     *	 numeric $xt = The expiration time from now in seconds. If it is negative, the absolute value is treated as the epoch time.
     *	 null $xt = No expiration time is specified.
     * Return:
     *   true = If success.
     *   false = If an error ocurred.
     */
    function cat( $key, $value, $xt = null )
    {
        assert('is_string($key)');
        assert('is_string($value)');
        assert('is_null($xt) or is_numeric($xt)');
        try { $this->api->append($key,$value,$xt); return true; }
        catch( RuntimeException $e ) { if( $this->runtime ) throw $e; else return false; }
    }

    /**
     * Add a record if it not exits.
     * Params:
     *	 string $key = The key of the record.
     *	 string $value = The value of the record.
     *	 numeric $xt = The expiration time from now in seconds. If it is negative, the absolute value is treated as the epoch time.
     *	 null $xt = No expiration time is specified.
     * Return:
     *	 true = If success.
     *	 false = If an error ocurred.
     *	 null = If the record already exists.
     */
    function add( $key, $value, $xt = null )
    {
        assert('is_string($key)');
        assert('is_string($value)');
        assert('is_null($xt) or is_numeric($xt)');
        try { return $this->api->add($key,$value,$xt); }
        catch( OutOfBoundsException $e ) { if( $this->outofbound ) throw $e; else return null; }
        catch( RuntimeException $e ) { if( $this->runtime ) throw $e; else return false; }
    }

    /**
     * Replace the value of a record.
     * Params:
     *	 string $key = The key of the record.
     *	 string $value = The value of the record.
     *	 numeric $xt = The expiration time from now in seconds. If it is negative, the absolute value is treated as the epoch time.
     *	 null $xt = No expiration time is specified.
     * Return:
     *	 true = If success.
     *	 false = If an error ocurred.
     *	 null = If the record don't exists.
     */
    function rep( $key, $value, $xt = null )
    {
        assert('is_string($key)');
        assert('is_string($value)');
        assert('is_null($xt) or is_numeric($xt)');
        try { return $this->api->replace($key,$value,$xt); }
        catch( OutOfBoundsException $e ) { if( $this->outofbound ) throw $e; else return null; }
        catch( RuntimeException $e ) { if( $this->runtime ) throw $e; else return false; }
    }

    /**
     * Remove the value of a record.
     * Params:
     *	 string $key = The key of the record.
     * Return:
     *	 true = If succes.
     *	 false = If an error ocurred.
     *	 null = If the record don't exists.
     */
    function del( $key )
    {
        assert('is_string($key)');
        try { $this->api->remove($key); return true; }
        catch( OutOfBoundsException $e ) { if( $this->outofbound ) throw $e; else return null; }
        catch( RuntimeException $e ) { if( $this->runtime ) throw $e; else return false; }
    }

    function del_bulk( $keys, $atomic = false )
    {
        assert('is_array($key)');
        try { return $this->api->remove_bulk($keys,$atomic); }
        catch( OutOfBoundsException $e ) { if( $this->outofbound ) throw $e; else return null; }
        catch( RuntimeException $e ) { if( $this->runtime ) throw $e; else return false; }
    }

    /**
     * Perform compare-and-swap.
     * Params:
     *	 string $key = The key of the record.
     *	 string $oval = The old value.
     *	 null $oval = If it is omittted, no record is meant.
     *	 string $nval = The new value.
     *	 null $nval = If it is omittted, the record is removed.
     *	 numeric $xt = The expiration time from now in seconds. If it is negative, the absolute value is treated as the epoch time.
     *	 null $xt = No expiration time is specified.
     * Return:
     *	 true = If success.
     *	 false = If an error ocurred.
     *	 null = If the old value assumption was failed.
     */
    function cas( $key, $oval, $nval, $xt = null )
    {
        assert('is_string($key)');
        assert('is_string($oval) or is_null($oval)');
        assert('is_string($nval) or is_null($nval)');
        assert('is_null($xt) or is_numeric($xt)');
        try { return $this->api->cas($key,$oval,$nval,$xt); }
        catch( OutOfBoundsException $e ) { if( $this->outofbound ) throw $e; else return null; }
        catch( RuntimeException $e ) { if( $this->runtime ) throw $e; else return false; }
    }

    // }}}
    // {{{ begin(), search(), forward(), backward(), prefix(), regex()

    function begin( $prefix, $max = 0, &$num = null )
    {
        assert('is_string($prefix)');
        assert('is_numeric($max) and $max>=0 and (int)$max==$max');
        $stm = clone $this;
        $stm->prefix = $prefix;
        $stm->backward = false;
        $stm->max = $max;
        $stm->num = &$num;
        return $stm;
    }

    function reverse_begin( $prefix, $max = 0, &$num = null )
    {
        assert('is_string($prefix)');
        assert('is_numeric($max) and $max>=0 and (int)$max==$max');
        $stm = clone $this;
        $stm->prefix = $prefix;
        $stm->backward = true;
        $stm->max = $max;
        $stm->num = &$num;
        return $stm;
    }

    /**
     * @param string $regex
     * @param int $max
     * @param null|int $num
     * @return UI|string[]
     */
    function search( $regex, $max = 0, &$num = null )
    {
        assert('is_string($regex)');
        assert('is_numeric($max) and $max>=0 and (int)$max==$max');
        $stm = clone $this;
        $stm->regex = $regex;
        $stm->backward = false;
        $stm->max = $max;
        $stm->num = &$num;
        return $stm;
    }

    function reverse_search( $regex, $max = 0, &$num = null )
    {
        assert('is_string($regex)');
        assert('is_numeric($max) and $max>=0 and (int)$max==$max');
        $stm = clone $this;
        $stm->regex = $regex;
        $stm->backward = true;
        $stm->max = $max;
        $stm->num = &$num;
        return $stm;
    }

    function forward( $key = null, $only_keys = false )
    {
        assert('is_string($key) or is_null($key)');
        assert('is_bool($only_keys)');
        $stm = clone $this;
        $stm->startkey = $key;
        $stm->backward = false;
        $stm->just_key = $only_keys;
        return $stm;
    }

    function backward( $key = null, $only_keys = false )
    {
        assert('is_string($key) or is_null($key)');
        $stm = clone $this;
        $stm->startkey = $key;
        $stm->backward = true;
        $stm->just_key = $only_keys;
        return $stm;
    }

    function prefix( $prefix, $max = 0, &$num = null )
    {
        assert('is_string($prefix)');
        assert('is_numeric($max) and $max>=0 and (int)$max==$max');
        $stm = clone $this;
        $stm->prefix = $prefix;
        $stm->just_key = true;
        $stm->max = $max;
        $stm->num = &$num;
        return $stm;
    }

    function regex( $regex, $max = 0, &$num = null )
    {
        assert('is_string($regex)');
        assert('is_numeric($max) and $max>=0 and (int)$max==$max');
        $stm = clone $this;
        $stm->regex = $regex;
        $stm->just_key = true;
        $stm->max = $max;
        $stm->num = &$num;
        return $stm;
    }

    // }}}
    // {{{ rewind(), current(), key(), next(), valid()

    /**
     * TODO check if integer limit is reach with cursor.
     */
    function rewind()
    {
        // If prefix is set, then retrieve the list of keys begin with this prefix.
        if( ! is_null($this->prefix) )
            $this->keys = $this->backward
                ? array_reverse( $this->api->match_prefix( $this->prefix, $this->max, $this->num ) )
                : $this->api->match_prefix( $this->prefix, $this->max, $this->num );
        // Else, if regex is set, then retrieve the list of keys that match this regex.
        elseif( ! is_null($this->regex) )
            $this->keys = $this->backward
                ? array_reverse( $this->api->match_regex( $this->regex, $this->max, $this->num ) )
                : $this->api->match_regex( $this->regex, $this->max, $this->num );
        // Else, the cursor will be use
        else
        {
            // If no cursor was set, the create a new one. It need to be uniq for each cURL session.
            if( is_null($this->cursor) )
            {
                if( ! $cursor = end(self::$cursors) ) $this->cursor = 1;
                else $this->cursor = $cursor+1;
                self::$cursors[$this->cursor] = $this->cursor;
            }
            // Now set the position of the cursor.
            try
            {
                assert('is_bool($this->backward)');
                if( $this->backward )
                    $this->api->cur_jump_back( $this->cursor, $this->startkey );
                else
                    $this->api->cur_jump( $this->cursor, $this->startkey );
            }
            catch( OutOfBoundsException $e ) {}
        }
    }

    function current()
    {
        assert('is_array($this->record)');
        if( ! is_null($this->prefix) or ! is_null($this->regex) or ! is_null($this->cursor) )
            return current($this->record);
        else
            return null;
    }

    function key()
    {
        assert('is_array($this->record)');
        if( ! is_null($this->prefix) or ! is_null($this->regex) or ! is_null($this->cursor) )
            return key($this->record);
        else
            return null;
    }

    function next()
    {
        if( ! is_null($this->prefix) or ! is_null($this->regex) )
        {
            assert('is_array($this->keys)');
            next($this->keys);
        }
        elseif( ! is_null($this->cursor) )
        {
            try
            {
                if( $this->backward )
                    $this->api->cur_step_back($this->cursor);
                else
                    $this->api->cur_step($this->cursor);
            }
            catch( OutOfBoundsException $e ) {}
        }
    }

    function valid()
    {
        if( ! is_null($this->prefix) or ! is_null($this->regex) )
        {
            assert('is_array($this->keys)');
            if( current($this->keys) )
                try
                {
                    if( $this->just_key )
                        return $this->record = array( key($this->keys) => current($this->keys) );
                    else
                        return $this->record = array( current($this->keys) => $this->get(current($this->keys)) );
                }
                catch( OutOfBoundsException $e ) { return false; }
            else
                return false;
        }
        elseif( ! is_null($this->cursor) )
        {
            try {
                if( $this->just_key )
                    return $this->record = array( $this->api->cur_get_key($this->cursor,false) );
                else
                    return $this->record = $this->api->cur_get($this->cursor,false);
            }
            catch( OutOfBoundsException $e ) { return false; }
        }
        else
            return false;
    }

    // }}}
    // {{{ to(), from()

    function to( $key, &$value )
    {
        assert('is_string($key)');
        $value = $this->get($key);
        return $this;
    }

    function from( $key, &$value = null )
    {
        assert('is_string($key)');
        $this->set($key,$value);
        return $this;
    }

    // }}}
    // {{{ scr()

    function scr( $name, $data = null )
    {
        assert('is_string($name)');
        assert('is_array($data) or is_null($data)');
        try { return $this->api->play_script($name,$data); }
        catch( OutOfBoundsException $e ) { if( $this->outofbound ) throw $e; else return null; }
        catch( RuntimeException $e ) { if( $this->runtime ) throw $e; else return false; }
    }

    // }}}
    // {{{ offsetExists(), offsetGet(), offsetSet(), offsetUnset()

    function offsetExists( $offset )
    {
        assert('is_string($offset)');
        try { return is_string($this->api->get($offset)); }
        catch( OutOfBoundsException $e ) { return false; }
        catch( RuntimeException $e ) { if( $this->runtime ) throw $e; else return false; }
    }

    function offsetGet( $offset )
    {
        assert('is_string($offset)');
        try { return $this->api->get($offset); }
        catch( OutOfBoundsException $e ) { if( $this->outofbound ) throw $e; else return null; }
        catch( RuntimeException $e ) { if( $this->runtime ) throw $e; else return false; }
    }

    function offsetSet( $offset, $value )
    {
        assert('is_string($offset)');
        assert('is_string($value)');
        try { $this->api->set($offset,$value); }
        catch( RuntimeException $e ) { if( $this->runtime ) throw $e; }
    }

    function offsetUnset( $offset )
    {
        assert('is_string($offset)');
        try { $this->api->remove($offset); }
        catch( OutOfBoundsException $e ) { if( $this->outofbound ) throw $e;  }
        catch( RuntimeException $e ) { if( $this->runtime ) throw $e; }
    }

    // }}}
}
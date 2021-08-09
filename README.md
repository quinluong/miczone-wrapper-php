# miczone-wrapper-php

## Installation

This library can be installed via Composer:

```bash
composer require quinluong/miczone-wrapper-php
```

## Test

```bash
./vendor/phpunit/phpunit/phpunit --verbose tests
./vendor/phpunit/phpunit/phpunit --repeat 10 --verbose tests
```

## EventBus

### Prerequisite libraries

In order to use this library, must be install by following https://github.com/arnaud-lb/php-rdkafka#installation

- Step 1: Install librdkafka
  https://github.com/edenhill/librdkafka
- Step 2: Install rdkafka
  https://arnaud.le-blanc.net/php-rdkafka-doc/phpdoc/rdkafka.installation.pecl.html

  Please choose version rdkafka-3.1.3 for high stability

### Usage

#### Notifier

```php
use Miczone\Wrapper\MiczoneEventBusClient\Notifier;
use Miczone\Thrift\EventBus\NotifyMessageResponse;

$client = new Notifier([
    'hosts' => '127.0.0.1:1234',
    'clientId' => 'my-service',
    'auth' => 'user:pass',
    'topics' => 'topicA,topicB',
    'numberOfRetries' => 3 // optional
]);

// Async
$client->ow_notifyString([
    'topic' => 'topicA',
    'key' => 'my-key', // to keep packages in order if they have the same key
    'data' => '{}' // data type can be Boolean / Integer / Double / String
], function () {
    // Success callback
}, function ($ex) {
    // Error callback
});

// Sync, type of $response is NotifyMessageResponse
$response = $client->notifyString([
    'topic' => 'topicB',
    'key' => 'my-key',
    'data' => '{}',
]);
```

#### Watcher

```php
use Miczone\Wrapper\MiczoneEventBusClient\Watcher;
use Miczone\Wrapper\MiczoneEventBusClient\WatcherHandler\StringWatcherHandlerInterface;

$client = new Watcher([
    'hosts' => '127.0.0.1:1234',
    'clientId' => 'my-service',
    'groupId' => 'my-group',
    'topics' => 'topicA,topicB'
]);


/**
 * Handler interface can be:
 * - BooleanWatcherHandlerInterface
 * - IntegerWatcherHandlerInterface
 * - DoubleWatcherHandlerInterface
 * - StringWatcherHandlerInterface
 */
$client->setHandler(new class implements StringWatcherHandlerInterface {
    public function onMessage(string $topic, int $partition, int $offset, int $timestamp, string $key = null, string $data = null) {
        // Do your stuff
    }

    public function onError($code, string $message, \Exception $exception = null) {
        // Catch error
    }
});

$client->start();
```

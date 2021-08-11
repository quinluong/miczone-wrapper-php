<?php

namespace Miczone\Wrapper\MiczoneCatalogClient;

use Miczone\Thrift\Catalog\Category\GetCategoryByIdRequest;
use Miczone\Thrift\Catalog\Category\GetCategoryByIdResponse;
use Miczone\Thrift\Catalog\Category\GetCategoryBySlugRequest;
use Miczone\Thrift\Catalog\Category\GetCategoryBySlugResponse;
use Miczone\Thrift\Catalog\Category\GetCategoryMappingByIdRequest;
use Miczone\Thrift\Catalog\Category\GetCategoryMappingByIdResponse;
use Miczone\Thrift\Catalog\Category\MultiGetCategoryByIdRequest;
use Miczone\Thrift\Catalog\Category\MultiGetCategoryByIdResponse;
use Miczone\Thrift\Catalog\Category\MultiGetCategoryBySlugRequest;
use Miczone\Thrift\Catalog\Category\MultiGetCategoryBySlugResponse;
use Miczone\Thrift\Catalog\MiczoneCatalogStorageServiceClient;
use Miczone\Thrift\Catalog\Product\GetProductByIdRequest;
use Miczone\Thrift\Catalog\Product\GetProductByIdResponse;
use Miczone\Thrift\Catalog\Product\GetSliceProductRequest;
use Miczone\Thrift\Catalog\Product\GetSliceProductResponse;
use Miczone\Thrift\Catalog\Product\MultiGetProductByIdRequest;
use Miczone\Thrift\Catalog\Product\MultiGetProductByIdResponse;
use Miczone\Thrift\Common\Error;
use Miczone\Thrift\Common\ErrorCode;
use Miczone\Thrift\Common\OperationHandle;
use Miczone\Thrift\Common\PoolSelectOption;
use Miczone\Thrift\Common\ScaleMode;
use Miczone\Wrapper\MiczoneClientBase;
use Thrift\Exception\TException;
use Thrift\Exception\TTransportException;
use Thrift\Protocol\TBinaryProtocol;
use Thrift\Transport\TFramedTransport;
use Thrift\Transport\TSocket;

class Storage extends MiczoneClientBase {

  const CLIENT_VERSION = 'v1.0';

  const HOSTS = [];

  const AUTH = '';

  const SEND_TIMEOUT_IN_MILLISECONDS = 1000;

  const RECEIVE_TIMEOUT_IN_MILLISECONDS = 2000;

  const NUMBER_OF_RETRIES = 1;

  const SCALE_MODE = ScaleMode::BALANCING;

  protected $lastException;

  private $_hostPorts;

  private $_hostPortsAliveStatus;

  private $_operationHandle;

  private $_sendTimeoutInMilliseconds;

  private $_receiveTimeoutInMilliseconds;

  private $_totalLoop;

  private $_poolSelectOption;

  public function __construct(array $config = []) {
    $config = array_merge([
      'hosts' => static::HOSTS,
      'auth' => static::AUTH,
      'sendTimeoutInMilliseconds' => static::SEND_TIMEOUT_IN_MILLISECONDS,
      'receiveTimeoutInMilliseconds' => static::RECEIVE_TIMEOUT_IN_MILLISECONDS,
      'numberOfRetries' => static::NUMBER_OF_RETRIES,
      'scaleMode' => static::SCALE_MODE,
    ], $config);

    $config['hostPorts'] = $this->standardizeHosts($config['hosts']);

    if (empty($config['hostPorts'])) {
      throw new \Exception('Invalid "hosts" config');
    }

    $this->_hostPorts = $config['hostPorts'];

    $this->_hostPortsAliveStatus = $this->initHostPortsAliveStatus($this->_hostPorts);

    $config['auth'] = $this->standardizeAuth($config['auth']);

    if (empty($config['auth'])) {
      throw new \Exception('Invalid "auth" config');
    }

    $this->_operationHandle = new OperationHandle([
      'username' => $config['auth']['username'],
      'password' => $config['auth']['password'],
    ]);

    if (!is_int($config['sendTimeoutInMilliseconds']) || $config['sendTimeoutInMilliseconds'] <= 0) {
      $config['sendTimeoutInMilliseconds'] = static::SEND_TIMEOUT_IN_MILLISECONDS;
    }

    $this->_sendTimeoutInMilliseconds = $config['sendTimeoutInMilliseconds'];

    if (!is_int($config['receiveTimeoutInMilliseconds']) || $config['receiveTimeoutInMilliseconds'] <= 0) {
      $config['receiveTimeoutInMilliseconds'] = static::RECEIVE_TIMEOUT_IN_MILLISECONDS;
    }

    $this->_receiveTimeoutInMilliseconds = $config['receiveTimeoutInMilliseconds'];

    if (!is_int($config['numberOfRetries']) || $config['numberOfRetries'] < 0) {
      $config['numberOfRetries'] = static::NUMBER_OF_RETRIES;
    }

    $this->_totalLoop = $config['numberOfRetries'] + 1;

    if (!is_int($config['scaleMode']) || ($config['scaleMode'] !== ScaleMode::BALANCING && $config['scaleMode'] !== ScaleMode::FAIL_OVER)) {
      $config['scaleMode'] = static::SCALE_MODE;
    }

    if ($config['scaleMode'] === ScaleMode::BALANCING) {
      $config['poolSelectOption'] = PoolSelectOption::ANY_ALIVE_OR_FIRST;
    } else {
      $config['poolSelectOption'] = PoolSelectOption::ALIVE_OR_FIRST;
    }

    $this->_poolSelectOption = $config['poolSelectOption'];
  }

  public function __destruct() {
  }

  private function _createTransportAndClient(string $host, int $port) {
    $socket = new TSocket($host, $port);
    $socket->setSendTimeout($this->_sendTimeoutInMilliseconds);
    $socket->setRecvTimeout($this->_receiveTimeoutInMilliseconds);

    $transport = new TFramedTransport($socket);

    $protocol = new TBinaryProtocol($transport);

    $client = new MiczoneCatalogStorageServiceClient($protocol);

    return [$transport, $client];
  }

  private function _getTransportAndClient() {
    $hostPorts = $this->_hostPorts;
    $hostPortsAliveStatus = $this->_hostPortsAliveStatus;
    $poolSize = count($hostPorts);
    $getFirstHostPort = false;

    if ($this->_poolSelectOption === PoolSelectOption::ANY_ALIVE_OR_FIRST) {
      $randomPoolIndex = rand(0, $poolSize - 1);

      for ($i = $randomPoolIndex; $i < $randomPoolIndex + $poolSize; ++$i) {
        $hostPort = $hostPorts[$i % $poolSize];

        if ($this->getHostPortAliveStatus($hostPortsAliveStatus, $hostPort) === false) {
          continue;
        }

        list($transport, $client) = $this->_createTransportAndClient($hostPort['host'], $hostPort['port']);

        if ($transport === null || $client === null) {
          continue;
        }

        return [$transport, $client, $hostPort];
      }

      $getFirstHostPort = true;
    } else if ($this->_poolSelectOption === PoolSelectOption::ALIVE_OR_FIRST) {
      for ($i = 0; $i < $poolSize; ++$i) {
        $hostPort = $hostPorts[$i];

        if ($this->getHostPortAliveStatus($hostPortsAliveStatus, $hostPort) === false) {
          continue;
        }

        list($transport, $client) = $this->_createTransportAndClient($hostPort['host'], $hostPort['port']);

        if ($transport === null || $client === null) {
          continue;
        }

        return [$transport, $client, $hostPort];
      }

      $getFirstHostPort = true;
    }

    if ($getFirstHostPort) {
      $hostPort = $hostPorts[0];

      list($transport, $client) = $this->_createTransportAndClient($hostPort['host'], $hostPort['port']);

      // Don't need to check null anymore

      return [$transport, $client, $hostPort];
    }

    throw new \Exception('Not supported yet');
  }

  public function setTraceId(string $value) {
    if ($value === null || trim($value) === '') {
      throw new \Exception('Invalid "value" param');
    }

    $this->_operationHandle->traceId = trim($value);
  }

  public function getLastException() {
    return $this->lastException;
  }

  private function _validateGetProductByIdRequest(GetProductByIdRequest $request) {
    if (!isset($request->id) || !is_string($request->id) || trim($request->id) === '') {
      throw new \Exception('Invalid "id" param');
    }

    $request->id = trim($request->id);
  }

  private function _validateMultiGetProductByIdRequest(MultiGetProductByIdRequest $request) {
    if (!isset($request->idList) || !is_array($request->idList) || count($request->idList) === 0) {
      throw new \Exception('Invalid "idList" param');
    }
  }

  private function _validateGetSliceProductRequest(GetSliceProductRequest $request) {
    if (isset($request->fromId) && (!is_string($request->fromId) || trim($request->fromId) === '')) {
      throw new \Exception('Invalid "fromId" param');
    }

    $request->fromId = trim($request->fromId);

    if (isset($request->limit) && (!is_int($request->limit) || $request->limit <= 0)) {
      throw new \Exception('Invalid "limit" param');
    }
  }

  private function _validateGetCategoryByIdRequest(GetCategoryByIdRequest $request) {
    if (!isset($request->id) || !is_string($request->id) || trim($request->id) === '') {
      throw new \Exception('Invalid "id" param');
    }

    $request->id = trim($request->id);
  }

  private function _validateMultiGetCategoryByIdRequest(MultiGetCategoryByIdRequest $request) {
    if (!isset($request->idList) || !is_array($request->idList) || count($request->idList) === 0) {
      throw new \Exception('Invalid "idList" param');
    }
  }

  private function _validateGetCategoryBySlugRequest(GetCategoryBySlugRequest $request) {
    if (!isset($request->slug) || !is_string($request->slug) || trim($request->slug) === '') {
      throw new \Exception('Invalid "slug" param');
    }

    $request->slug = trim($request->slug);
  }

  private function _validateMultiGetCategoryBySlugRequest(MultiGetCategoryBySlugRequest $request) {
    if (!isset($request->slugList) || !is_array($request->slugList) || count($request->slugList) === 0) {
      throw new \Exception('Invalid "slugList" param');
    }
  }

  private function _validateGetCategoryMappingByIdRequest(GetCategoryMappingByIdRequest $request) {
    if (!isset($request->id) || !is_string($request->id) || trim($request->id) === '') {
      throw new \Exception('Invalid "id" param');
    }

    $request->id = trim($request->id);

    if (isset($request->childDepth) && (!is_int($request->childDepth) || $request->childDepth < 0)) {
      throw new \Exception('Invalid "childDepth" param');
    }
  }

  /**
   * @return \Miczone\Thrift\Common\ErrorCode
   */
  public function ping() {
    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $result = $client->ping($this->_operationHandle);
        $transport->close();

        $this->markHostPortAlive($this->_hostPortsAliveStatus, $hostPort);

        return $result;
      } catch (TTransportException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (TException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (\Exception $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      }
    }

    return ErrorCode::THRIFT_BAD_REQUEST;
  }

  /**
   * @param \Miczone\Thrift\Catalog\Product\GetProductByIdRequest
   * @return \Miczone\Thrift\Catalog\Product\GetProductByIdResponse
   * @throws \Exception
   */
  public function getProductById(GetProductByIdRequest $request) {
    $this->_validateGetProductByIdRequest($request);

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $result = $client->getProductById($this->_operationHandle, $request);
        $transport->close();

        $this->markHostPortAlive($this->_hostPortsAliveStatus, $hostPort);

        return $result;
      } catch (TTransportException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (TException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (\Exception $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      }
    }

    return new GetProductByIdResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param \Miczone\Thrift\Catalog\Product\MultiGetProductByIdRequest
   * @return \Miczone\Thrift\Catalog\Product\MultiGetProductByIdResponse
   * @throws \Exception
   */
  public function multiGetProductByIdList(MultiGetProductByIdRequest $request) {
    $this->_validateMultiGetProductByIdRequest($request);

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $result = $client->multiGetProductById($this->_operationHandle, $request);
        $transport->close();

        $this->markHostPortAlive($this->_hostPortsAliveStatus, $hostPort);

        return $result;
      } catch (TTransportException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (TException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (\Exception $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      }
    }

    return new MultiGetProductByIdResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param \Miczone\Thrift\Catalog\Product\GetSliceProductRequest
   * @return \Miczone\Thrift\Catalog\Product\GetSliceProductResponse
   * @throws \Exception
   */
  public function getSliceProduct(GetSliceProductRequest $request) {
    $this->_validateGetSliceProductRequest($request);

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $result = $client->getSliceProduct($this->_operationHandle, $request);
        $transport->close();

        $this->markHostPortAlive($this->_hostPortsAliveStatus, $hostPort);

        return $result;
      } catch (TTransportException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (TException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (\Exception $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      }
    }

    return new GetSliceProductResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param \Miczone\Thrift\Catalog\Category\GetCategoryByIdRequest
   * @return \Miczone\Thrift\Catalog\Category\GetCategoryByIdResponse
   * @throws \Exception
   */
  public function getCategoryById(GetCategoryByIdRequest $request) {
    $this->_validateGetCategoryByIdRequest($request);

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $result = $client->getCategoryById($this->_operationHandle, $request);
        $transport->close();

        $this->markHostPortAlive($this->_hostPortsAliveStatus, $hostPort);

        return $result;
      } catch (TTransportException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (TException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (\Exception $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      }
    }

    return new GetCategoryByIdResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param \Miczone\Thrift\Catalog\Category\MultiGetCategoryByIdRequest
   * @return \Miczone\Thrift\Catalog\Category\MultiGetCategoryByIdResponse
   * @throws \Exception
   */
  public function multiGetCategoryByIdList(MultiGetCategoryByIdRequest $request) {
    $this->_validateMultiGetCategoryByIdRequest($request);

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $result = $client->multiGetCategoryById($this->_operationHandle, $request);
        $transport->close();

        $this->markHostPortAlive($this->_hostPortsAliveStatus, $hostPort);

        return $result;
      } catch (TTransportException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (TException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (\Exception $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      }
    }

    return new MultiGetCategoryByIdResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param \Miczone\Thrift\Catalog\Category\GetCategoryBySlugRequest
   * @return \Miczone\Thrift\Catalog\Category\GetCategoryBySlugResponse
   * @throws \Exception
   */
  public function getCategoryBySlug(GetCategoryBySlugRequest $request) {
    $this->_validateGetCategoryBySlugRequest($request);

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $result = $client->getCategoryBySlug($this->_operationHandle, $request);
        $transport->close();

        $this->markHostPortAlive($this->_hostPortsAliveStatus, $hostPort);

        return $result;
      } catch (TTransportException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (TException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (\Exception $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      }
    }

    return new GetCategoryBySlugResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param \Miczone\Thrift\Catalog\Category\MultiGetCategoryBySlugRequest
   * @return \Miczone\Thrift\Catalog\Category\MultiGetCategoryBySlugResponse
   * @throws \Exception
   */
  public function multiGetCategoryBySlugList(MultiGetCategoryBySlugRequest $request) {
    $this->_validateMultiGetCategoryBySlugRequest($request);

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $result = $client->multiGetCategoryBySlug($this->_operationHandle, $request);
        $transport->close();

        $this->markHostPortAlive($this->_hostPortsAliveStatus, $hostPort);

        return $result;
      } catch (TTransportException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (TException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (\Exception $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      }
    }

    return new MultiGetCategoryBySlugResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param \Miczone\Thrift\Catalog\Category\GetCategoryMappingByIdRequest
   * @return \Miczone\Thrift\Catalog\Category\GetCategoryMappingByIdResponse
   * @throws \Exception
   */
  public function getCategoryMappingById(GetCategoryMappingByIdRequest $request) {
    $this->_validateGetCategoryMappingByIdRequest($request);

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $result = $client->getCategoryMappingById($this->_operationHandle, $request);
        $transport->close();

        $this->markHostPortAlive($this->_hostPortsAliveStatus, $hostPort);

        return $result;
      } catch (TTransportException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (TException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (\Exception $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      }
    }

    return new GetCategoryMappingByIdResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

}

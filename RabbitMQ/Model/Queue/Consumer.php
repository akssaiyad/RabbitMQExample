<?php

namespace Aks\RabbitMQ\Model\Queue;

/**
 * Class Consumer
 * @package Aks\RabbitMQ\Model\Queue
 */
class Consumer
{
    
    /* @var \Magento\Framework\Serialize\Serializer\Json  */     
    protected $_json;
 
    /**
    * @param string $orders
    */
    public function process($orders)
    {
        try{
            $this->execute($orders);
            
        }catch (\Exception $e){
            $errorCode = $e->getCode();
            $message = __('Something went wrong while adding orders to queue');
            $this->_notifier->addCritical(
                $errorCode,
                $message
            );
            $this->_logger->critical($errorCode .": ". $message);
        }
    }
 
    /**
    * @param $orderItems
    *
    * @throws LocalizedException
    */
    private function execute($orderItems)
    {
        $orderCollectionArr = [];
        /* @var \Aks\RabbitMQ\Model\Queue $queue */
        $queue = $this->_queueFactory->create();
        $orderItems = $this->_json->unserialize($orderItems);
        if(is_array($orderItems)){
            foreach ($orderItems as $type => $orderId) {
            $orderCollectionArr[] = [
                    'type' => 'order',
                    'entity_id' => $orderId,
                    'priority' => 1,
                ];
            }
            //handle insertMulti orders into QueueExample queue
            $queue->add($orderCollectionArr);
        }
    }
}
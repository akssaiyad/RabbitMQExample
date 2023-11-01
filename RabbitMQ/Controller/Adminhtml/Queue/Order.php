<?php
namespace Aks\RabbitMQ\Controller\Adminhtml\Queue;
 
/**
 * Class Order
 * @package Codilar\QueueExample\Controller\Adminhtml\Queue
 */
class Order extends \Magento\Backend\App\Action
{
	/**
 	* Authorization level of a basic admin session
 	*/
	const ADMIN_RESOURCE = 'Aks_RabbitMQ::config_aksqueueexample';
 
	const TOPIC_NAME = 'aksqueueexample.queue.order';
 
	const SIZE = 5000;
 
	/* @var \Magento\Sales\Model\ResourceModel\Order\CollectionFactory  /
	protected $_orderColFactory;
 
	/* @var \Magento\Framework\Serialize\Serializer\Json  /
	protected $_json;
 
	/* @var \Magento\Framework\MessageQueue\PublisherInterface  /
	protected $_publisher;
 
	/**
 	* Order constructor.
 	*
 	* @param \Magento\Sales\Model\ResourceModel\Order\CollectionFactory $orderColFactory
 	* @param \Magento\Framework\MessageQueue\PublisherInterface $publisher
 	* @param \Magento\Framework\Serialize\Serializer\Json $json
 	* @param \Magento\Backend\App\Action\Context $context
 	*/
	public function __construct(
    	\Magento\Sales\Model\ResourceModel\Order\CollectionFactory $orderColFactory,
    	\Magento\Framework\MessageQueue\PublisherInterface $publisher,
    	\Magento\Framework\Serialize\Serializer\Json $json,
    	\Magento\Backend\App\Action\Context $context
	){
    	$this->_orderColFactory = $orderColFactory;
    	$this->_json = $json;
    	$this->_publisher = $publisher;
	}
 
	/**
 	* @return \Magento\Framework\App\ResponseInterface|\Magento\Framework\Controller\ResultInterface|void
 	*/
	public function execute()
	{
    	if ($this->getRequest()->isAjax()) {
        	try {
				//get list of order IDs
				$orderCollection = $this->_orderColFactory->create()->addFieldToSelect('entity_id')->getAllIds();
				//send data to publish function
            	$this->publishData($orderCollection, $this->type);
            	$this->getResponse()->setBody($this->_json->serialize([
                	'error' => 0,
                	'message' => __('Orders are being added to queue')
            	]));
            	return;
        	} catch (\Exception $e) {
            	$this->getResponse()->setBody($this->_json->serialize([
                	'error' => 0, 
                	'message' => __('Something went wrong while adding record(s) to queue. Error: '.$e->getMessage())
            	]));
            	return;
        	}
    	}
    	return $this->_redirect('*/*/index');
	}
 
	/**
 	* @param $data
 	* @param $type
 	*/
	public function publishData($data,$type)
	{
    	if(is_array($data)){
		//split list of IDs into arrays of 5000 IDs each
        	$chunks = array_chunk($data,self::SIZE);
        	foreach ($chunks as $chunk){
			//publish IDs to queue
			$rawData = [$type => $chunk];
			$this->_publisher->publish(self::TOPIC_NAME, $this->_json->serialize($rawData));
        	}
    	}
	}
}
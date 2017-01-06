(ql:quickload :manardb)
(manardb:use-mmap-dir "~/test-class/")

(manardb:defmmclass files ()
  ((file :type STRING :initarg :file)))

(manardb:defmmclass additionalEventData ()
  ((value :type STRING :initarg :value :accessor value)))

(manardb:defmmclass awsRegion ()
  ((value :type STRING :initarg :value :accessor value)))

(manardb:defmmclass errorCode ()
  ((value :type STRING :initarg :value :accessor value)))

(manardb:defmmclass errorMessage ()
  ((value :type STRING :initarg :value :accessor value)))

(manardb:defmmclass eventID ()
  ((value :type STRING :initarg :value :accessor value)))

(manardb:defmmclass eventName ()
  ((value :type STRING :initarg :value :accessor value)))

(manardb:defmmclass eventSource ()
  ((value :type STRING :initarg :value :accessor value)))

(manardb:defmmclass eventTime ()
  ((value :type STRING :initarg :value :accessor value)))

(manardb:defmmclass eventType ()
  ((value :type STRING :initarg :value :accessor value)))

(manardb:defmmclass eventVersion ()
  ((value :type STRING :initarg :value :accessor value)))

(manardb:defmmclass recipientAccountId ()
  ((value :type STRING :initarg :value :accessor value)))

(manardb:defmmclass requestID ()
  ((value :type STRING :initarg :value :accessor value)))

(manardb:defmmclass requestParameters ()
  ((value :type STRING :initarg :value :accessor value)))

(manardb:defmmclass resources ()
  ((value :type STRING :initarg :value :accessor value)))

(manardb:defmmclass responseElements ()
  ((value :type STRING :initarg :value :accessor value)))

(manardb:defmmclass sourceIPAddress ()
  ((value :type STRING :initarg :value :accessor value)))

(manardb:defmmclass userAgent ()
  ((value :type STRING :initarg :value :accessor value)))

(manardb:defmmclass userIdentity ()
  ((value :type STRING :initarg :value :accessor value)))

(manardb:defmmclass userName ()
  ((value :type STRING :initarg :value :accessor value)))

(manardb:defmmclass ct ()
  ((additionalEventData :type additionalEventData :initarg :additionalEventData :accessor additionalEventData)
   (awsRegion :type awsRegion :initarg :awsRegion :accessor awsRegion)
   (errorCode :type errorCode :initarg :errorCode :accessor errorCode)
   (errorMessage :type errorMessage :initarg :errorMessage :accessor errorMessage)
   (eventID :type eventID :initarg :eventID :accessor eventID)
   (eventName :type eventName :initarg :eventName :accessor eventName)
   (eventSource :type eventSource :initarg :eventSource :accessor eventSource)
   (eventTime :type eventTime :initarg :eventTime :accessor eventTime)
   (eventType :type eventType :initarg :eventType :accessor eventType)
   (eventVersion :type eventVersion :initarg :eventVersion :accessor eventVersion)
   (recipientAccountId :type recipientAccountId :initarg :recipientAccountId :accessor recipientAccountId)
   (requestID :type requestID :initarg :requestID :accessor requestID)
   (requestParameters :type requestParameters :initarg :requestParameters :accessor requestParameters)
   (resources :type resources :initarg :resources :accessor resources)
   (responseElements :type responseElements :initarg :responseElements :accessor responseElements)
   (sourceIPAddress :type sourceIPAddress :initarg :sourceIPAddress :accessor sourceIPAddress)
   (userAgent :type userAgent :initarg :userAgent :accessor userAgent)
   (userIdentity :type userIdentity :initarg :userIdentity :accessor userIdentity)
   (userName :type userName :initarg :userName :accessor username)
   ))

(let
    ((additionalEventData-i (make-instance 'additionalEventData :value "foo1"))
     (awsRegion-i (make-instance 'awsRegion :value "foo2"))
     (errorCode-i (make-instance 'errorCode :value "foo3"))
     (errorMessage-i (make-instance 'errorMessage :value "foo4"))
     (eventID-i (make-instance 'eventID :value "foo5"))
     (eventName-i (make-instance 'eventName :value "foo6"))
     (eventSource-i (make-instance 'eventSource :value "foo7"))
     (eventTime-i (make-instance 'eventTime :value "foo7"))
     (eventType-i (make-instance 'eventType :value "foo8"))
     (eventVersion-i (make-instance 'eventVersion :value "foo8"))
     (recipientAccountId-i (make-instance 'recipientAccountId :value "foo9"))
     (requestID-i (make-instance 'requestID :value "foo"))
     (requestParameters-i (make-instance 'requestParameters :value "foo"))
     (resources-i (make-instance 'resources :value "foo"))
     (responseElements-i (make-instance 'responseElements :value "foo"))
     (sourceIPAddress-i (make-instance 'sourceIPAddress :value "foo"))
     (userAgent-i (make-instance 'userAgent :value "foo"))
     (userIdentity-i (make-instance 'userIdentity :value "foo"))
     (userName-i (make-instance 'userName :value "foo")))

  (make-instance 'ct
		 :additionalEventData additionalEventData-i
		 :awsRegion awsRegion-i
		 :errorCode errorCode-i
		 :errorMessage errorMessage-i
		 :eventID eventID-i
		 :eventName eventName-i
		 :eventSource eventSource-i
		 :eventTime eventTime-i
		 :eventType eventType-i
		 :eventVersion eventVersion-i
		 :recipientAccountId recipientAccountId-i
		 :requestID requestID-i
		 :requestParameters requestParameters-i
		 :resources resources-i
		 :responseElements responseElements-i
		 :sourceIPAddress sourceIPAddress-i
		 :userAgent userAgent-i
		 :userIdentity userIdentity-i
		 :userName userName-i
		 ))

(slot-value (slot-value (first (manardb:retrieve-all-instances 'ct)) 'awsRegion) 'value)

(defun get-entry (class value)
  (let ((entry nil))
    (manardb:doclass (x class :fresh-instances nil)
      (if (string-equal value (slot-value x value))
      (setf entry
	  (

  )

(in-package :metis)
(manardb:use-mmap-dir "~/ct-manardb/")
(ql:quickload :manardb)
(manardb:defmmclass ct ()
   ((additionalEventData :type STRING :initarg :additionalEventData)
   (awsRegion :type STRING :initarg :awsRegion)
   (errorCode :type STRING :initarg :errorCode)
   (errorMessage :type STRING :initarg :errorMessage)
   (eventID :type STRING :initarg :eventID)
   (eventName :type STRING :initarg :eventName)
   (eventSource :type STRING :initarg :eventSource)
   (eventTime :type STRING :initarg :eventTime)
   (eventType :type STRING :initarg :eventType)
   (eventVersion :type STRING :initarg :eventVersion)
   (recipientAccountId :type STRING :initarg :recipientAccountId)
   (requestID :type STRING :initarg :requestID)
   (requestParameters :type STRING :initarg :requestParameters)
   (resources :type STRING :initarg :resources)
   (responseElements :type STRING :initarg :responseElements)
   (sourceIPAddress :type STRING :initarg :sourceIPAddress)
   (userAgent :type STRING :initarg :userAgent)
   (userIdentity :type STRING :initarg :userIdentity)
   (userName :type STRING :initarg :userName)))

(defun manardb-recreate-tables ()
  (format t "manardb-recreate-tables~%"))

(defun manardb-normalize-insert (record)
  ;;manardb-nomalize-insert (NIL us-west-1 NIL NIL 216e957f-230e-42ea-bfc7-e0d07d321a8b DescribeDBInstances rds.amazonaws.com 2015-08-07T19:04:52Z AwsApiCall 1.03 224108527019 2f1d4165-3d37-11e5-aae4-c1965b0823e9 NIL NIL NIL bogus.example.com signin.amazonaws.com (invokedBy signin.amazonaws.com sessionContext (attributes (creationDate 2015-08-07T11:17:07Z mfaAuthenticated true)) userName meylor accessKeyId ASIAIOHLZS2V2QON52LA accountId 224108527019 arn arn:aws:iam::224108527019:user/meylor principalId AIDAJVKKNU5BSTZIOF3EU type IAMUser) meylor)
  (destructuring-bind ( additionalEventData
		       awsRegion
		       errorCode
		       errorMessage
		       eventID
		       eventName
		       eventSource
		       eventTime
		       eventType
		       eventVersion
		       recipientAccountId
		       requestID
		       requestParameters
		       resources
		       responseElements
		       sourceIPAddress
		       userAgent
		       userIdentity
		       userName)
      record
    (make-instance 'ct
		   :additionalEventData additionalEventData
		   :awsRegion awsRegion
		   :errorCode errorCode
		   :errorMessage errorMessage
		   :eventID eventID
		   :eventName eventName
		   :eventSource eventSource
		   :eventTime eventTime
		   :eventType eventType
		   :eventVersion eventVersion
		   :recipientAccountId recipientAccountId
		   :requestID requestID
		   :requestParameters requestParameters
		   :resources resources
		   :responseElements responseElements
		   :sourceIPAddress sourceIPAddress
		   :userAgent userAgent
		   :userIdentity userIdentity
		   :userName userName)
    )
  (print (length (manardb:retrieve-all-instances 'ct)))
  )


(defun manardb-get-or-insert-id (table value)
  (format t "manard-get-or-insert-id table:~A value:~A~%" table value)
  )

(defun manardb-drop-table (query)
  (format t "manardb-drop-table query:~A~%" query)
  )

(defun manardb-do-query (query)
  (format t "manardb-do-query query:~A~%" query)
  )

(in-package :metis)

;; fundamental metis ops
(defun ssdb/init ()
  (unless ssdb:*connection*
    (ssdb:connect)))

(defun ssdb/close ()
  (ssdb:disconnect))

(defun ssdb/have-we-seen-this-file (file)
  (ssdb:exists (format nil "F-~a" (get-filename-hash file))))

(defun ssdb/mark-file-processed (file)
  (ssdb:set (format nil "F-~a" (get-filename-hash file)) "1"))

(defun ssdb/normalize-insert (record)
  (destructuring-bind (
                       additionalEventData
                       apiVersion
                       awsRegion
                       errorCode
                       errorMessage
                       eventCategory
                       eventID
                       eventName
                       eventSource
                       eventTime
                       eventType
                       eventVersion
                       managementEvent
                       readOnly
                       recipientAccountId
                       requestID
                       requestParameters
                       resources
                       responseElements
                       serviceEventDetails
                       sessionCredentialFromConsole
                       sharedEventID
                       sourceIPAddress
                       tlsDetails
                       userAgent
                       userIdentity
                       userName
                       vpcEndpointId
                       )
      record
    (ssdb:multi_hset eventID
                     "additionalEventData" additionalEventData
                     "apiVersion" apiVersion
                     "awsRegion" awsRegion
                     "errorCode" errorCode
                     "errorMessage" errorMessage
                     "eventCategory" eventCategory
                     "eventName" eventName
                     "eventSource" eventSource
                     "eventTime" eventTime
                     "eventType" eventType
                     "eventVersion" eventVersion
                     "managementEvent" managementEvent
                     "readOnly" readOnly
                     "recipientAccountId" recipientAccountId
                     "requestID" requestID
                     "requestParameters" requestParameters
                     "resources" resources
                     "responseElements" responseElements
                     "serviceEventDetails" serviceEventDetails
                     "sessionCredentialFromConsole" sessionCredentialFromConsole
                     "sharedEventID" sharedEventID
                     "sourceIPAddress" sourceIPAddress
                     "tlsDetails" tlsDetails
                     "userAgent" userAgent
                     "userIdentity" userIdentity
                     "userName" userName
                     "vpcEndpointId" vpcEndpointId)))

;; ported kunabi style ops

(defun ssdb/db-key? (key)
  (ssdb:exists key))

(defun ssdb/db-get (key)
  (ssdb:get key))

(defun ssdb/get-stats ()
  (format t "~a" (ssdb:info)))

(defun ssdb/get-unique-names ()
  (ssdb/get-unique "userName"))

(defun ssdb/get-unique-events ()
  (ssdb/get-unique "eventName"))

(defun ssdb/get-unique (field)
  (let* ((records (time (ssdb:hlist "" "" -1)))
         (items (time (mapcar (lambda (record)
                          (ssdb:hget record field))
                        records)))
         (uniqs (time (sort-uniq items))))
    (format t "~{~a~%~}" uniqs)))

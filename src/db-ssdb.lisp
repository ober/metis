(in-package :metis)

;; fundamental metis ops
(defun ssdb/init ()
  (unless ssdb:*connection*
    (ssdb:connect)))

(defun ssdb/close ()
  (ssdb:flushdb)
  (ssdb:disconnect))

(defun ssdb/have-we-seen-this-file (file)
  (ssdb:exists (format nil "F-~a" file)))

(defun ssdb/mark-file-processed (file)
  (ssdb:set (format nil "F-~a" file) "1"))

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
    (format t "userAgent~a~%" userAgent)))
    ;; (let ((additionalEventData-i (get-idx 'metis::additionalEventData additionalEventData))
    ;;       (apiVersion-i (get-idx 'metis::apiVersion apiVersion))
    ;;       (awsRegion-i (get-idx 'metis::awsRegion awsRegion))
    ;;       (errorCode-i (get-idx 'metis::errorCode errorCode))
    ;;       (errorMessage-i (get-idx 'metis::errorMessage errorMessage))
    ;;       (eventCategory-i (get-idx 'metis::eventCategory eventCategory ))
    ;;       (eventID-i (get-idx 'metis::eventID eventID))
    ;;       (eventName-i (get-idx 'metis::eventName eventName))
    ;;       (eventSource-i (get-idx 'metis::eventSource eventSource))
    ;;       (eventTime-i (get-idx 'metis::eventTime eventTime))
    ;;       (eventType-i (get-idx 'metis::eventType eventType))
    ;;       (eventVersion-i (get-idx 'metis::eventVersion eventVersion))
    ;;       (managementEvent-i (get-idx 'metis::managementEvent managementEvent))
    ;;       (readOnly-i (get-idx 'metis::readOnly  readOnly))
    ;;       (recipientAccountId-i (get-idx 'metis::recipientAccountId recipientAccountId))
    ;;       (requestID-i (get-idx 'metis::requestID requestID))
    ;;       (requestParameters-i (get-idx 'metis::requestParameters requestParameters))
    ;;       (resources-i (get-idx 'metis::resources resources))
    ;;       (responseElements-i (get-idx 'metis::responseElements  responseElements))
    ;;       (serviceEventDetails-i (get-idx 'metis::serviceEventDetails serviceEventDetails))
    ;;       (sessionCredentialFromConsole-i (get-idx 'metis::sessionCredentialFromConsole sessionCredentialFromConsole))
    ;;       (sharedEventID-i (get-idx 'metis::sharedEventID sharedEventID))
    ;;       (sourceIPAddress-i (get-idx 'metis::sourceIPAddress sourceIPAddress))
    ;;       (tlsDetails-i (get-idx 'metis::tlsDetails tlsDetails))
    ;;       (userAgent-i (get-idx 'metis::userAgent userAgent))
    ;;       (userIdentity-i (get-idx 'metis::userIdentity userIdentity))
    ;;       (userName-i (get-idx 'metis::userName (or userName (find-username userIdentity))))
    ;;       (vpcEndpointId-i (get-idx 'metis::vpcEndpointId vpcEndpointId)))
    ;;   (make-instance 'ct
    ;;                  :additionalEventData additionalEventData-i
    ;;                  :apiVersion apiVersion-i
    ;;                  :awsRegion awsRegion-i
    ;;                  :errorCode errorCode-i
    ;;                  :errorMessage errorMessage-i
    ;;                  :eventCategory eventCategory-i
    ;;                  :eventID eventID-i
    ;;                  :eventName eventName-i
    ;;                  :eventSource eventSource-i
    ;;                  :eventTime eventTime-i
    ;;                  :eventType eventType-i
    ;;                  :eventVersion eventVersion-i
    ;;                  :managementEvent managementEvent-i
    ;;                  :readOnly readOnly-i
    ;;                  :recipientAccountId recipientAccountId-i
    ;;                  :requestID requestID-i
    ;;                  :requestParameters requestParameters-i
    ;;                  :resources resources-i
    ;;                  :responseElements responseElements-i
    ;;                  :serviceEventDetails serviceEventDetails-i
    ;;                  :sessionCredentialFromConsole sessionCredentialFromConsole-i
    ;;                  :sharedEventID sharedEventID-i
    ;;                  :sourceIPAddress sourceIPAddress-i
    ;;                  :tlsDetails tlsDetails-i
    ;;                  :userAgent userAgent-i
    ;;                  :userIdentity userIdentity-i
    ;;                  :userName userName-i
    ;;                  :vpcEndpointId vpcEndpointId-i


;; ported kunabi style ops

(defun ssdb/db-key? (key)
  (ssdb:exists key))

(defun ssdb/db-get (key)
  (ssdb:get key))

;; kv related

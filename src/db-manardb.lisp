(in-package :metis)
(defvar *manard-files* (thread-safe-hash-table))
(defvar *metis-fields* (thread-safe-hash-table))
(defvar *metis-counters* (thread-safe-hash-table))
(defvar *metis-need-files* nil)
(defvar *output-sep* "|")

(defvar ct-fields '(
                    ;;metis::additionalEventData
                    ;;metis::eventID
                    ;;metis::recipientAccountId
                    ;;metis::requestID
                    ;;metis::requestParameters
                    ;;metis::resources
                    ;;metis::responseElements
                    ;;metis::userIdentity
                    metis::eventCategory
                    metis::serviceEventDetails
                    metis::readOnly
                    metis::managementEvent
                    ;;metis::sharedEventID
                    ;;metis::tlsDetails
                    metis::vpcEndpointId
                    metis::sessionCredentialFromConsole
                    metis::apiVersion
                    metis::awsRegion
                    metis::errorCode
                    metis::errorMessage
                    metis::eventName
                    metis::eventSource
                    ;;metis::eventTime
                    metis::eventType
                    metis::eventVersion
                    metis::sourceIPAddress
                    metis::userAgent
                    metis::userName
                    ))

(defun manardb-init()
  (unless (boundp 'manardb:use-mmap-dir)
    (progn
      (manardb:use-mmap-dir (or (uiop:getenv "METIS") "~/ct-manardb/"))
      (when *metis-need-hashes*
        (init-ct-hashes))))
  (if (and (eql (hash-table-count *manard-files*) 0) *metis-need-files*)
      (allocate-file-hash)))

(manardb:defmmclass files ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass eventCategory ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass readOnly ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass serviceEventDetails ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass managementEvent ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass sharedEventID ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass tlsDetails ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass vpcEndpointId ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass apiVersion ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass sessionCredentialFromConsole ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass additionalEventData ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass awsRegion ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass errorCode ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass errorMessage ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass eventID ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass eventName ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass eventSource ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass eventTime ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass eventType ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass eventVersion ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass recipientAccountId ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass requestID ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass requestParameters ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass resources ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass responseElements ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass sourceIPAddress ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass userAgent ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass userIdentity ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass userName ()
  ((value :initarg :value :accessor value)
   (idx :initarg :idx :accessor idx)))

(manardb:defmmclass ct ()
  ((addionalEventData :initarg :additionalEventData :accessor additionalEventData)
   (apiVersion :initarg :apiVersion :accessor apiVersion)
   (awsRegion :initarg :awsRegion :accessor awsRegion)
   (errorCode :initarg :errorCode :accessor errorCode)
   (errorMessage :initarg :errorMessage :accessor errorMessage)
   (eventCategory :initarg :eventCategory :accessor eventCategory)
   (eventID :initarg :eventID :accessor eventID)
   (eventName :initarg :eventName :accessor eventName)
   (eventSource :initarg :eventSource :accessor eventSource)
   (eventTime :initarg :eventTime :accessor eventTime)
   (eventType :initarg :eventType :accessor eventType)
   (eventVersion :initarg :eventVersion :accessor eventVersion)
   (managementEvent :initarg :managementEvent :accessor managementEvent)
   (readOnly :initarg :readOnly :accessor readOnly)
   (recipientAccountId :initarg :recipientAccountId :accessor recipientAccountId)
   (requestID :initarg :requestID :accessor requestID)
   (requestParameters :initarg :requestParameters :accessor requestParameters)
   (resources :initarg :resources :accessor resources)
   (responseElements :initarg :responseElements :accessor responseElements)
   (serviceEventDetails :initarg :serviceEventDetails :accessor serviceEventDetails)
   (sessionCredentialFromConsole :initarg :sessionCredentialFromConsole :accessor sessionCredentialFromConsole)
   (sharedEventID :initarg :sharedEventID :accessor sharedEventID)
   (sourceIPAddress :initarg :sourceIPAddress :accessor sourceIPAddress)
   (tlsDetails :initarg :tlsDetails :accessor tlsDetails)
   (userAgent :initarg :userAgent :accessor userAgent)
   (userIdentity :initarg :userIdentity :accessor userIdentity)
   (userName :initarg :userName :accessor username)
   (vpcEndpointId :initarg :vpcEndpointId :accessor vpcEndpointId)))

(defun create-klass-hash (klass)
  (multiple-value-bind (id seen)
      (gethash klass *metis-fields*)
    (unless seen
      (setf (gethash klass *metis-fields*)
            (thread-safe-hash-table)))))

(defun get-obj (klass new-value)
  "Return the object for a given value of klass"
  (let ((obj nil))
    (unless (or (null klass) (null new-value))
      (progn
        (create-klass-hash klass)
        (multiple-value-bind (id seen)
            (gethash new-value (gethash klass *metis-fields*))
          (if seen
              (setf obj id)
              (progn
                (setf obj (make-instance klass :value new-value))
                (setf (gethash new-value (gethash klass *metis-fields*)) obj))))))
    obj))

(defun manardb-have-we-seen-this-file (file)
  (let ((name (get-filename-hash file)))
    (multiple-value-bind (id seen)
        (gethash name *manard-files*)
      seen)))

(defun manardb-get-files (file)
  (format t "manardb-get-files:~A~%" file)
  (remove-if-not
   (lambda (x) (string-equal
           (get-filename-hash file)
           (slot-value x :value2)))
   (manardb:retrieve-all-instances 'metis::files)))

(defun manardb-mark-file-processed (file)
  (let ((name (get-filename-hash file)))
    (setf (gethash name *manard-files*) t)
    (make-instance 'files :value name :idx 1)))

(defun allocate-file-hash ()
  (manardb:doclass (x 'metis::files :fresh-instances nil)
                   (setf (gethash (slot-value x 'value) *manard-files*) t)))

(defun allocate-klass-hash (klass)
  (or (hash-table-p (gethash klass *metis-fields*))
      (progn
        (format t "allocating class:~A~%" klass)
        (create-klass-hash klass)
        (manardb:doclass (x klass :fresh-instances nil)
                         (with-slots (value idx) x
                           (setf (gethash value
                                          (gethash klass *metis-fields*)) idx)))
        (setf (gethash klass *metis-counters*)
              (get-max-id-from-hash
               (gethash klass *metis-fields*))))))

(defun get-max-id-from-hash (hash)
  (let* ((idxs (alexandria:hash-table-values hash))
         (max-id 0))
    (and idxs
         (setf max-id (+ 1 (hash-table-count hash))))
    max-id))

(defun init-ct-hashes ()
  (mapc
   #'(lambda (x)
       (time (allocate-klass-hash x)))
   ct-fields))

(defun get-stats ()
  (format t "Totals ct:~A files:~A flows:~A vpc-files:~A ec:~A srcaddr:~A dstaddr:~A srcport:~A dstport:~A protocol:~A~%"
          (manardb:count-all-instances 'metis::ct)
          (manardb:count-all-instances 'metis::files)
          (manardb:count-all-instances 'metis::flow)
          (manardb:count-all-instances 'metis::flow-files)
          (manardb:count-all-instances 'metis::errorCode)
          (manardb:count-all-instances 'metis::srcaddr)
          (manardb:count-all-instances 'metis::dstaddr)
          (manardb:count-all-instances 'metis::srcport)
          (manardb:count-all-instances 'metis::dstport)
          (manardb:count-all-instances 'metis::protocol)))

(defun find-username (userIdentity)
  (let ((a (fetch-value '(:|sessionContext| :|sessionIssuer| :|userName|) userIdentity))
        (b (fetch-value '(:|sessionContext| :|userName|) userIdentity))
        (c (fetch-value '(:|userName|) userIdentity))
        (d (car (last (cl-ppcre:split ":" (fetch-value '(:|arn|) userIdentity)))))
        (e (fetch-value '(:|type|) userIdentity))
        (len (length userIdentity)))
    (or a b c d e)))

(defun get-unique-values (klass)
  "Return unique list of klass objects"
  (manardb:doclass (x klass :fresh-instances nil)
                   (with-slots (value idx) x
                     (format t "~%~A: ~A" idx value))))

(defun get-unique-values-list (klass)
  "Return unique list of klass objects"
  (let ((results '()))
    (manardb:doclass (x klass :fresh-instances nil)
                     (with-slots (value idx) x
                       (push value results)))
    results))

;; lists

(defun get-serviceEventDetails ()
  "return unique list of serviceEventDetails"
  (get-unique-values 'metis::serviceEventDetails))

(defun get-eventCategory ()
  "return unique list of eventCategory"
  (get-unique-values 'metis::eventCategory))

(defun get-readOnly ()
  "return unique list of readOnly"
  (get-unique-values 'metis::readOnly))

(defun get-managementEvent ()
  "return unique list of managementEvent"
  (get-unique-values 'metis::managementEvent))

(defun get-sharedEventID ()
  "return unique list of sharedEventID"
  (get-unique-values 'metis::sharedEventID))

(defun get-tlsDetails ()
  "return unique list of tlsDetails"
  (get-unique-values 'metis::tlsDetails))

(defun get-vpcEndpointId ()
  "return unique list of vpcEndpointId"
  (get-unique-values 'metis::vpcEndpointId))

(defun get-apiVersion ()
  "return unique list of apiVersion"
  (get-unique-values 'metis::apiVersion))

(defun get-ct-files ()
  "Return unique list of ct files"
  (get-unique-values 'metis::files))

(defun get-cts ()
  "Return unique cts"
  (get-unique-values 'metis::ct))

(defun get-event-list ()
  "Return unique list of events"
  (get-unique-values 'metis::eventname))

(defun get-errorcode-list ()
  "Return unique list of events"
  (get-unique-values 'metis::errorcode))

(defun get-response-elements ()
  "Return unique list of events"
  (get-unique-values 'metis::responseElements))

(defun get-name-list ()
  "Return unique list of events"
  (get-unique-values 'metis::username))

(defun get-region-list ()
  "Return unique list of events"
  (get-unique-values 'metis::awsRegion))

(defun get-sourceips-list ()
  "Return unique list of events"
  (get-unique-values 'metis::sourceIPAddress))

(defun get-val (obj)
  (if (null obj)
      obj
      (slot-value obj 'value)))

(defun get-obj-by-val (klass val)
  (let ((obj-list nil))
    (manardb:doclass (x klass :fresh-instances nil)
                     (with-slots (value) x
                       (if (string-equal val value)
                           (push x obj-list))))
    obj-list))

(defun ct-get-by-klass-value  (klass value &optional inverse)
  (format t "~{~A~}" (ct-get-by-klass-value-real klass value (or inverse nil))))

(defun ct-get-by-klass-value-real (klass value &optional inverse)
  (allocate-klass-hash klass)
  (format t "klass:~A value:~A inverse:~A~%" klass value inverse)
  (let ((results '())
        (klass-hash (gethash klass *metis-fields*))
        (slotv nil))
    (multiple-value-bind (id seen)
        (gethash value klass-hash)
      (when (or seen inverse)
        (manardb:doclass
         (x 'metis::ct :fresh-instances nil)
         (with-slots (userName
                      eventTime
                      eventName
                      eventSource
                      sourceIPAddress
                      userAgent
                      errorMessage
                      errorCode
                      userIdentity
                      requestParameters
                      responseElements
                      awsRegion) x
           (cond
             ((equal (find-class klass) (find-class 'metis::userName)) (setf slotv userName))
             ((equal (find-class klass) (find-class 'metis::awsRegion)) (setf slotv awsRegion))
             ((equal (find-class klass) (find-class 'metis::eventName)) (setf slotv eventName))
             ((equal (find-class klass) (find-class 'metis::eventSource)) (setf slotv eventSource))
             ((equal (find-class klass) (find-class 'metis::sourceIPAddress)) (setf slotv sourceIPAddress))
             ((equal (find-class klass) (find-class 'metis::errorMessage)) (setf slotv errorMessage))
             ((equal (find-class klass) (find-class 'metis::errorCode)) (setf slotv errorCode))
             ((equal (find-class klass) (find-class 'metis::requestParameters)) (setf slotv requestParameters))
             ((equal (find-class klass) (find-class 'metis::responseElements)) (setf slotv responseElements))
             )
           (when
               (or
                (and inverse slotv)
                (and slotv (ignore-errors (= slotv id))))
             (push
              (format nil "|~{~A | ~}~%"
                      (list
                       (get-val-by-idx 'metis::eventTime eventTime)
                       (get-val-by-idx 'metis::eventName eventName)
                       (get-val-by-idx 'metis::userName userName)
                       (get-val-by-idx 'metis::eventSource eventSource)
                       (get-val-by-idx 'metis::sourceIPAddress sourceIPAddress)
                       (get-val-by-idx 'metis::userAgent userAgent)
                       (get-val-by-idx 'metis::errorMessage errorMessage)
                       (get-val-by-idx 'metis::errorCode errorCode)
                       (cleanup-output (cl-ppcre:regex-replace-all "\\n" (format nil "~A" (get-val-by-idx 'metis::requestParameters requestParameters)) ""))
                       (cleanup-output (cl-ppcre:regex-replace-all "\\n" (format nil "~A" (get-val-by-idx 'metis::responseElements responseElements)) ""))
                       (get-val-by-idx 'metis::userName userName)
                       ;;(find-username (get-val-by-idx 'metis::userIdentity userIdentity))
                       ))
              results))))))
    results))


(defun cleanup-output (str)
  (let* ((no-dupes (cl-ppcre:regex-replace-all "[\\t ]+" str " "))
         (no-returns (cl-ppcre:regex-replace-all "\\n" no-dupes " "))
         no-returns)))

;; Search functions

(defun ct-get-all-errors ()
  (ct-get-by-klass-value 'metis::errorCode nil t))

(defun ct-get-by-name (name)
  (ct-get-by-klass-value 'metis::userName name))

(defun ct-get-by-region (region)
  (ct-get-by-klass-value 'metis::awsRegion region))

(defun ct-get-by-errorcode (name)
  (ct-get-by-klass-value 'metis::errorCode name))

(defun ct-get-by-errorMessage (name)
  (ct-get-by-klass-value 'metis::errorMessage name))

(defun ct-get-by-eventName (name)
  (ct-get-by-klass-value 'metis::eventName name))

(defun ct-get-by-eventSource (name)
  (ct-get-by-klass-value 'metis::eventSource name))

(defun ct-get-by-sourceIPAddress (name)
  (ct-get-by-klass-value 'metis::sourceIPAddress name))

(defun manardb-normalize-insert (record)
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
    (let ((additionalEventData-i (get-idx 'metis::additionalEventData additionalEventData))
          (apiVersion-i (get-idx 'metis::apiVersion apiVersion))
          (awsRegion-i (get-idx 'metis::awsRegion awsRegion))
          (errorCode-i (get-idx 'metis::errorCode errorCode))
          (errorMessage-i (get-idx 'metis::errorMessage errorMessage))
          (eventCategory-i (get-idx 'metis::eventCategory eventCategory ))
          (eventID-i (get-idx 'metis::eventID eventID))
          (eventName-i (get-idx 'metis::eventName eventName))
          (eventSource-i (get-idx 'metis::eventSource eventSource))
          (eventTime-i (get-idx 'metis::eventTime eventTime))
          (eventType-i (get-idx 'metis::eventType eventType))
          (eventVersion-i (get-idx 'metis::eventVersion eventVersion))
          (managementEvent-i (get-idx 'metis::managementEvent managementEvent))
          (readOnly-i (get-idx 'metis::readOnly  readOnly))
          (recipientAccountId-i (get-idx 'metis::recipientAccountId recipientAccountId))
          (requestID-i (get-idx 'metis::requestID requestID))
          (requestParameters-i (get-idx 'metis::requestParameters requestParameters))
          (resources-i (get-idx 'metis::resources resources))
          (responseElements-i (get-idx 'metis::responseElements  responseElements))
          (serviceEventDetails-i (get-idx 'metis::serviceEventDetails serviceEventDetails))
          (sessionCredentialFromConsole-i (get-idx 'metis::sessionCredentialFromConsole sessionCredentialFromConsole))
          (sharedEventID-i (get-idx 'metis::sharedEventID sharedEventID))
          (sourceIPAddress-i (get-idx 'metis::sourceIPAddress sourceIPAddress))
          (tlsDetails-i (get-idx 'metis::tlsDetails tlsDetails))
          (userAgent-i (get-idx 'metis::userAgent userAgent))
          (userIdentity-i (get-idx 'metis::userIdentity userIdentity))
          (userName-i (get-idx 'metis::userName (or userName (find-username userIdentity))))
          (vpcEndpointId-i (get-idx 'metis::vpcEndpointId vpcEndpointId)))
      (make-instance 'ct
                     :additionalEventData additionalEventData-i
                     :apiVersion apiVersion-i
                     :awsRegion awsRegion-i
                     :errorCode errorCode-i
                     :errorMessage errorMessage-i
                     :eventCategory eventCategory-i
                     :eventID eventID-i
                     :eventName eventName-i
                     :eventSource eventSource-i
                     :eventTime eventTime-i
                     :eventType eventType-i
                     :eventVersion eventVersion-i
                     :managementEvent managementEvent-i
                     :readOnly readOnly-i
                     :recipientAccountId recipientAccountId-i
                     :requestID requestID-i
                     :requestParameters requestParameters-i
                     :resources resources-i
                     :responseElements responseElements-i
                     :serviceEventDetails serviceEventDetails-i
                     :sessionCredentialFromConsole sessionCredentialFromConsole-i
                     :sharedEventID sharedEventID-i
                     :sourceIPAddress sourceIPAddress-i
                     :tlsDetails tlsDetails-i
                     :userAgent userAgent-i
                     :userIdentity userIdentity-i
                     :userName userName-i
                     :vpcEndpointId vpcEndpointId-i
                     ))))

(defun cleanse (var)
  (typecase var
    (null (string var))
    (string var)
    (list (format nil "~{~s = ~s~%~}" var))))

;; noops

(defun manardb-recreate-tables ()
  (format t "manardb-recreate-tables~%"))

(defun manardb-get-or-insert-id (table value)
  (format t "manard-get-or-insert-id table:~A value:~A~%" table value))

(defun manardb-drop-table (query)
  (format t "manardb-drop-table query:~A~%" query) )

(defun manardb-do-query (query)
  (format nil "manardb-do-query query:~A~%" query))

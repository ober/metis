(in-package :metis)
;;(declaim (optimize (speed 3) (debug 0) (safety 0) (compilation-speed 0)))
(defvar *manard-files* (make-hash-table :test 'equalp))
(defvar *metis-fields* (make-hash-table :test 'equalp))

(defun init-manardb()
  (unless (boundp 'manardb:use-mmap-dir)
    (manardb:use-mmap-dir "~/ct-manardb/"))
  (if (eql (hash-table-count *manard-files*) 0)
      (allocate-file-hash)))

(manardb:defmmclass files ()
  ((file :type STRING :initarg :file :accessor file)))

(manardb:defmmclass additionalEventData ()
  ((value :initarg :value :accessor value)))

(manardb:defmmclass awsRegion ()
  ((value :initarg :value :accessor value)))

(manardb:defmmclass errorCode ()
  ((value :initarg :value :accessor value)))

(manardb:defmmclass errorMessage ()
  ((value :initarg :value :accessor value)))

(manardb:defmmclass eventID ()
  ((value :initarg :value :accessor value)))

(manardb:defmmclass eventName ()
  ((value :initarg :value :accessor value)))

(manardb:defmmclass eventSource ()
  ((value :initarg :value :accessor value)))

(manardb:defmmclass eventTime ()
  ((value :initarg :value :accessor value)))

(manardb:defmmclass eventType ()
  ((value :initarg :value :accessor value)))

(manardb:defmmclass eventVersion ()
  ((value :initarg :value :accessor value)))

(manardb:defmmclass recipientAccountId ()
  ((value :initarg :value :accessor value)))

(manardb:defmmclass requestID ()
  ((value :initarg :value :accessor value)))

(manardb:defmmclass requestParameters ()
  ((value :initarg :value :accessor value)))

(manardb:defmmclass resources ()
  ((value :initarg :value :accessor value)))

(manardb:defmmclass responseElements ()
  ((value :initarg :value :accessor value)))

(manardb:defmmclass sourceIPAddress ()
  ((value :initarg :value :accessor value)))

(manardb:defmmclass userAgent ()
  ((value :initarg :value :accessor value)))

(manardb:defmmclass userIdentity ()
  ((value :initarg :value :accessor value)))

(manardb:defmmclass userName ()
  ((value :initarg :value :accessor value)))

(manardb:defmmclass ct ()
  ((addionalEventData :initarg :additionalEventData :accessor additionalEventData)
   (awsRegion :initarg :awsRegion :accessor awsRegion)
   (errorCode :initarg :errorCode :accessor errorCode)
   (errorMessage :initarg :errorMessage :accessor errorMessage)
   (eventID :initarg :eventID :accessor eventID)
   (eventName :initarg :eventName :accessor eventName)
   (eventSource :initarg :eventSource :accessor eventSource)
   (eventTime :initarg :eventTime :accessor eventTime)
   (eventType :initarg :eventType :accessor eventType)
   (eventVersion :initarg :eventVersion :accessor eventVersion)
   (recipientAccountId :initarg :recipientAccountId :accessor recipientAccountId)
   (requestID :initarg :requestID :accessor requestID)
   (requestParameters :initarg :requestParameters :accessor requestParameters)
   (resources :initarg :resources :accessor resources)
   (responseElements :initarg :responseElements :accessor responseElements)
   (sourceIPAddress :initarg :sourceIPAddress :accessor sourceIPAddress)
   (userAgent :initarg :userAgent :accessor userAgent)
   (userIdentity :initarg :userIdentity :accessor userIdentity)
   (userName :initarg :userName :accessor username)
   ))

(fare-memoization:define-memo-function get-obj (klass new-value)
  "Return the object for a given value of klass"
  (let ((obj nil))
    (unless (or (null klass) (null new-value))
      (progn
	(multiple-value-bind (id1 seen1)
	    (gethash klass *metis-fields*)
	  (unless seen1
	    (setf (gethash klass *metis-fields*)
		  (make-hash-table :test 'equalp))))
	(multiple-value-bind (id seen)
	    (gethash new-value (gethash klass *metis-fields*))
	  (if seen
	      (setf obj id)
	      (progn
		(setf obj (make-instance klass :value new-value))
		(setf (gethash new-value (gethash klass *metis-fields*)) obj))))))
    ;;(format t "get-obj: klass:~A new-value:~A obj:~A~%" klass new-value obj)
    obj))

(defun manardb-have-we-seen-this-file (file)
  (let ((name (get-filename-hash file)))
    (multiple-value-bind (val before)
	(gethash name *manard-files*)
	(format t "seen:~A ?:~A:~A~%" name val before))
    (multiple-value-bind (id seen)
	(gethash name *manard-files*)
      seen)))

(defun manardb-get-files (file)
  (remove-if-not
   (lambda (x) (string-equal (get-filename-hash file) (slot-value x 'file)))
   (manardb:retrieve-all-instances 'metis::files)))

(defun manardb-mark-file-processed (file)
  (let ((name (get-filenamre-hash file)))
    (setf (gethash name *manard-files*) t)
    (make-instance 'files :file name)))

(defun print-record-a (x)
  (format t "|~A|~A|~A|~A|~A|~A|~A|~%"
	  (slot-value x 'eventTime)
	  (slot-value x 'eventName)
	  (slot-value x 'eventSource)
	  (slot-value x 'sourceIPAddress)
	  (slot-value x 'userAgent)
	  (slot-value x 'errorMessage)
	  (slot-value x 'errorCode)
	  ;;(slot-value x 'userIdentity)
	  ))

(defun allocate-file-hash ()
  (manardb:doclass (x 'metis::files :fresh-instances nil)
		   (setf (gethash (slot-value x 'file) *manard-files*) t)))

(defun get-stats ()
  (format t "Totals ct:~A files:~A flows:~A vpc-files:~A ec:~A convs:~A~%"
	  (manardb:count-all-instances 'metis::ct)
	  (manardb:count-all-instances 'metis::files)
	  (manardb:count-all-instances 'metis::flow)
	  (manardb:count-all-instances 'metis::flow-files)
	  (manardb:count-all-instances 'metis::errorCode)
	  (manardb:count-all-instances 'metis::conversation)
	  ))

(fare-memoization:define-memo-function  find-username (userIdentity)
  (let ((a (fetch-value '(:|sessionContext| :|sessionIssuer| :|userName|) userIdentity))
	(b (fetch-value '(:|sessionContext| :|userName|) userIdentity))
	(c (fetch-value '(:|userName|) userIdentity))
	(d (fetch-value '(:|type|) userIdentity))
	(len (length userIdentity)))
    ;;(format t "a: ~A b:~A c:~A d:~A len:~A username:~A" a b c d len username)
    (or a b c d)))

(defun get-all-errorcodes ()
  (manardb:doclass
   (x 'metis::ct :fresh-instances nil)
   (unless (string-equal "NIL" (get-val (slot-value x 'errorCode)))
     (with-slots (userName eventTime eventName eventSource sourceIPAddress userAgent errorMessage errorCode userIdentity) x
       (format t "|~A|~A|~A|~A|~A|~A|~A|~A|~%"
	       (get-val eventTime)
	       (get-val errorCode)
	       (get-val userName)
	       (get-val eventName)
	       (get-val eventSource)
	       (get-val sourceIPAddress)
	       (get-val userAgent)
	       (get-val errorMessage))))))

;;(cl-ppcre:regex-replace #\newline 'userIdentity " "))))))

(defun get-unique-values (klass)
  "Return uniqure list of klass objects"
  (let ((values nil))
    (manardb:doclass (x klass :fresh-instances nil)
		     (with-slots (value) x
		       (push value values)))
    (format t "~{~A~^~%~}" (delete-duplicates (sort values #'string-lessp) :test 'string-equal))))

;; uniques

(defun get-event-list ()
  "Return uniqure list of events"
  (get-unique-values 'metis::eventname))

(defun get-errorcode-list ()
  "Return uniqure list of events"
  (get-unique-values 'metis::errorcode))

(defun get-name-list ()
  "Return uniqure list of events"
  (get-unique-values 'metis::username))

(defun get-sourceips-list ()
  "Return uniqure list of events"
  (get-unique-values 'metis::sourceIPAddress))

(fare-memoization:define-memo-function get-val (obj)
  (if (null obj)
      obj
      (slot-value obj 'value)))

(defun get-obj-by-val (klass val)
  (let ((obj-list nil))
    ;;(let ((obj nil))
    (manardb:doclass (x klass :fresh-instances nil)
		     (with-slots (value) x
		       (if (string-equal val value)
			   ;;(setf obj x))))
			   (push x obj-list))))
    ;;obj))
    obj-list))

(defun get-by-name (val)
  (manardb:doclass (x 'metis::ct :fresh-instances nil)
		   (with-slots (userName eventTime eventName eventSource sourceIPAddress userAgent errorMessage errorCode userIdentity) x
		     (let ((val2 (get-val userName)))
		       (if (string-equal val val2)
			   (format t "|~A|~A|~A|~A|~A|~A|~A|~%"
				   (get-val eventTime)
				   val2
				   (get-val eventSource)
				   (get-val sourceIPAddress)
				   (get-val userAgent)
				   (get-val errorMessage)
				   (get-val errorCode))
			   )))))

(defun get-by-event (val)
  (manardb:doclass (x 'metis::ct :fresh-instances nil)
		   (with-slots (userName eventTime eventName eventSource sourceIPAddress userAgent errorMessage errorCode userIdentity) x
		     (let ((val2 (slot-value eventname 'value)))
		       (if (string-equal val val2)
			   (format t "|~A|~A|~A|~A|~A|~A|~A|~%"
				   (get-val eventTime)
				   (get-val userName)
				   (get-val eventSource)
				   (get-val sourceIPAddress)
				   (get-val userAgent)
				   (get-val errorMessage)
				   (get-val errorCode))
			   )))))

(defun get-by-errorcode (val)
  (manardb:doclass (x 'metis::ct :fresh-instances nil)
		   (with-slots (userName eventTime eventName eventSource sourceIPAddress userAgent errorMessage errorCode userIdentity) x
		     (let ((val2 (slot-value errorcode 'value)))
		       (if (string-equal val val2)
			   (format t "|~A|~A|~A|~A|~A|~A|~A|~%"
				   (get-val eventTime)
				   (get-val userName)
				   (get-val eventSource)
				   (get-val sourceIPAddress)
				   (get-val userAgent)
				   (get-val errorMessage)
				   (get-val errorCode))
			   )))))

(defun get-by-date (val)
  (manardb:doclass (x 'metis::ct :fresh-instances nil)
		   (with-slots (userName eventTime eventName eventSource sourceIPAddress userAgent errorMessage errorCode userIdentity) x
		     (let ((val2 (get-val eventTime)))
		       (if (cl-ppcre:all-matches val val2)
			   (format t "|~A|~A|~A|~A|~A|~A|~A|~%"
				   val2
				   (get-val userName)
				   (get-val eventSource)
				   (get-val sourceIPAddress)
				   (get-val userAgent)
				   (get-val errorMessage)
				   (get-val errorCode))
			   )))))

(defun get-by-sourceip (val)
  (manardb:doclass (x 'metis::ct :fresh-instances nil)
		   (with-slots (userName eventTime eventName eventSource sourceIPAddress userAgent errorMessage errorCode userIdentity) x
		     (let ((val2 (slot-value sourceipaddress 'value)))
		       (if (string-equal val val2)
			   (format t "|~A|~A|~A|~A|~A|~A|~A|~%"
				   (get-val eventTime)
				   (get-val userName)
				   (get-val eventSource)
				   (get-val sourceIPAddress)
				   (get-val userAgent)
				   (get-val errorMessage)
				   (get-val errorCode))
			   )))))

(defun get-by-ip (val)
  (manardb:doclass (x 'metis::ct :fresh-instances nil)
		   (with-slots (userName eventTime eventName eventSource sourceIPAddress userAgent errorMessage errorCode userIdentity) x
		     (let ((val2 (slot-value eventSource 'value)))
		       (if (string-equal val val2)
			   (format t "|~A|~A|~A|~A|~A|~A|~A|~%"
				   (get-val eventTime)
				   (get-val userName)
				   (get-val eventSource)
				   (get-val sourceIPAddress)
				   (get-val userAgent)
				   (get-val errorMessage)
				   (get-val errorCode))
			   )))))

;; (let ((obj-list (get-obj-by-val 'username name)))
;;   (manardb:doclass (x 'metis::ct :fresh-instances nil)
;;     (with-slots (userName eventTime eventName eventSource sourceIPAddress userAgent errorMessage errorCode userIdentity) x
;; 	(let ((name2 (slot-value userName 'value)))
;; 	  (if (string-equal name2 name)
;; 	      (format t "|~A|~A|~A|~A|~A|~A|~A|~%"
;; 		      (get-val eventTime)
;; 		      name2
;; 		      (get-val eventSource)
;; 		      (get-val sourceIPAddress)
;; 		      (get-val userAgent)
;; 		      (get-val errorMessage)
;; 		      (get-val errorCode))
;; 	      ))))))

(defun get-useridentity-by-name (name)
  "Return any entries with username in useridentity"
  (manardb:doclass (x 'metis::ct :fresh-instances nil)
		   (with-slots (userName eventTime eventName eventSource sourceIPAddress userAgent errorMessage errorCode userIdentity) x
		     (if (cl-ppcre:all-matches name userIdentity)
			 (format t "|~A|~A|~A|~A|~A|~A|~A|~A|~%"
				 eventTime
				 errorCode
				 userName
				 eventName
				 eventSource
				 sourceIPAddress
				 userAgent
				 errorMessage)))))

(defun manardb-recreate-tables ()
  (format t "manardb-recreate-tables~%"))

(defun manardb-normalize-insert-2 (record)
  ;;manardb-nomalize-insert (NIL us-west-1 NIL NIL 216e957f-230e-42ea-bfc7-e0d07d321a8b DescribeDBInstances rds.amazonaws.com 2015-08-07T19:04:52Z AwsApiCall 1.03 224108527019 2f1d4165-3d37-11e5-aae4-c1965b0823e9 NIL NIL NIL bogus.example.com signin.amazonaws.com (invokedBy signin.amazonaws.com sessionContext (attributes (creationDate 2015-08-07T11:17:07Z mfaAuthenticated true)) userName meylor accessKeyId ASIAIOHLZS2V2QON52LA accountId 224108527019 arn arn:aws:iam::224108527019:user/meylor principalId AIDAJVKKNU5BSTZIOF3EU type IAMUser) meylor)
  (destructuring-bind (
		       additionalEventData
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
		       userName
		       )
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
		   :userName userName
		   )
    ))


(defun manardb-normalize-insert (record)
  (declare (optimize (speed 3) (debug 0) (safety 0) (compilation-speed 0)))
  ;;manardb-nomalize-insert (NIL us-west-1 NIL NIL 216e957f-230e-42ea-bfc7-e0d07d321a8b DescribeDBInstances rds.amazonaws.com 2015-08-07T19:04:52Z AwsApiCall 1.03 224108527019 2f1d4165-3d37-11e5-aae4-c1965b0823e9 NIL NIL NIL bogus.example.com signin.amazonaws.com (invokedBy signin.amazonaws.com sessionContext (attributes (creationDate 2015-08-07T11:17:07Z mfaAuthenticated true)) userName meylor accessKeyId ASIAIOHLZS2V2QON52LA accountId 224108527019 arn arn:aws:iam::224108527019:user/meylor principalId AIDAJVKKNU5BSTZIOF3EU type IAMUser) meylor)
  (destructuring-bind (
		       additionalEventData
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
		       userName
		       )
      record
    (let (
	  (additionalEventData-i (get-obj 'metis::additionalEventData additionalEventData))
	  (awsRegion-i (get-obj 'metis::awsRegion awsRegion))
	  (errorCode-i (get-obj 'metis::errorCode errorCode))
	  (errorMessage-i (get-obj 'metis::errorMessage errorMessage))
	  (eventID-i (get-obj 'metis::eventID eventID))
	  (eventName-i (get-obj 'metis::eventName eventName))
	  (eventSource-i (get-obj 'metis::eventSource eventSource))
	  (eventTime-i (get-obj 'metis::eventTime eventTime))
	  (eventType-i (get-obj 'metis::eventType eventType))
	  (eventVersion-i (get-obj 'metis::eventVersion eventVersion))
	  (recipientAccountId-i (get-obj 'metis::recipientAccountId recipientAccountId))
	  (requestID-i (get-obj 'metis::requestID requestID))
	  (requestParameters-i (get-obj 'metis::requestParameters requestParameters))
	  (resources-i (get-obj 'metis::resources resources))
	  (responseElements-i (get-obj 'metis::responseElements responseElements))
	  (sourceIPAddress-i (get-obj 'metis::sourceIPAddress sourceIPAddress))
	  (userAgent-i (get-obj 'metis::userAgent userAgent))
	  (userIdentity-i (get-obj 'metis::userIdentity userIdentity))
	  (userName-i (get-obj 'metis::userName (or userName (find-username userIdentity)))))

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
		     ))))


(defun cleanse (var)
  (typecase var
    (null (string var))
    (string var)
    (list (format nil "~{~s = ~s~%~}" var))))

(defun manardb-get-or-insert-id (table value)
  (format t "manard-get-or-insert-id table:~A value:~A~%" table value)
  )

(defun manardb-drop-table (query)
  (format t "manardb-drop-table query:~A~%" query)
  )

(defun manardb-do-query (query)
  (format nil "manardb-do-query query:~A~%" query)
  )

(defun add-val(val)
  (unless (string? val)
    val)
  (when (string? val)
    (dp (format "add-val: ~a" val))
    (let ((seen (db-key? val))
          (hcn 0))
      (if seen
          (set! hcn (db-get val))
          (begin
           (inc-hc)
           (set! hcn HC)
           (db-put val HC)
           (db-put (format "~a" HC) val)))
      hcn)))

(defun get-next-id (max)
  (let ((maxid (1+ max)))
    (if (db-key? (format "~a" maxid))
      (get-next-id (* 2 maxid))
      maxid)))

(defun inc-hc()
  ;; increment HC to next free id.
  (let ((next (get-next-id HC)))
    (setq HC next)
    (db-put "HC" (format "~a" HC))))

(defun (flush-indices-hash)
  (let ((indices (make-hash-table)))
    (for (index (hash-keys indices-hash))
      (dp (format "fih: index: ~a" index))
      (db-put (format "I-~a" index) (hash-get indices-hash index))
      (hash-put! indices index #t))
    (db-put "INDICES" indices)))

(defun list-index-entries(idx)
  (if (db-key? idx)
    (let ((entries (hash-keys (db-get idx))))
      (if (list? entries)
	      (for-each
	        (lambda (x)
	          (displayln x))
	        (sort! entries eq?))
	      (begin
	        (displayln "did not get list back from entries")
	        (type-of entries))))
    (displayln "no idx found for " idx)))

(defun add-val (val)
  (unless (string? val)
    val)
  (when (string? val)
    (dp (format nil "add-val: ~a" val))
    (let ((seen (db-key? val))
          (hcn 0))
      (if seen
          (setq hcn (db-get val))
          (progn
            (inc-hc)
            (setq hcn HC)
            (db-put val HC)
            (db-put (format nil "~a" HC) val)))
      hcn)))

(defun get-next-id (max)
  (let ((maxid (1+ max)))
    (if (db-key? (format nil "~a" maxid))
        (get-next-id (* 2 maxid))
        maxid)))

(defun inc-hc()
  ;; increment HC to next free id.
  (let ((next (get-next-id HC)))
    (setq HC next)
    (db-put "HC" (format "~a" HC))))

(defun flush-indices-hash()
  (let ((indices (make-hash-table)))
    (for:for (index in (hash-table-keys indices-hash))
             (dp (format nil "fih: index: ~a" index))
             (db-put (format nil "I-~a" index) (gethash index indices-hash))
             (hash-put! indices index t))
    (db-put "INDICES" indices)))

(defun list-index-entries(idx)
  (if (db-key? idx)
      (let ((entries (hash-table-keys (db-get idx))))
        (if (list? entries)
            (for:for (x in (sort entries #'string<=))
                     (displayln x))
            (progn
              (format t "did not get list back from entries~%")
              (type-of entries))))
      (format t "no idx found for ~a~%" idx)))

(defun resolve-records (ids)
  (let ((records '()))
    (for:for (id in ids)
             (let ((record (db-get (format nil "~a" id))))
               (when record
                 (push record records))))
    records))

(defun add-to-index (index entry)
  (dp (format "in add-to-index index: ~a entry: ~a" index entry))
  (let ((index-in-global-hash? (hash-key? indices-hash index)))
    (dp (format  "index-in-global-hash? ~a ~a" index-in-global-hash? index))
    (if index-in-global-hash?
        (new-index-entry index entry)
        (begin
         (dp (format "ati: index not in global hash for ~a. adding" index))
         (hash-put! indices-hash index (hash))
         (let ((have-db-entry-for-index (db-key? (format "I-~a" index))))
           (dp (format "have-db-entry-for-index: ~a key: I-~a" have-db-entry-for-index index))
           (if have-db-entry-for-index
               (update-db-index index entry)
               (new-db-index index entry)))))))

(defun new-index-entry (index entry)
  "Add entry to index in global hash"
  (dp (format "new-index-entry: ~a ~a" index entry))
  (unless (hash-key? (hash-get indices-hash index) entry)
    (hash-put! (hash-get indices-hash index) entry t)))

(defun new-db-index (index entry)
  "New index, with entry to db"
  (dp (format "new-db-index: ~a ~a" index entry))
  (let ((current (make-hash-table)))
    (hash-put! current entry t)
    (hash-put! indices-hash index current)
    (db-put (format "I-~a" index) current)))

(defun update-db-index (index entry)
  "Fetch the index from db, then add our new entry, and save."
  (dp (format "update-db-index: ~a ~a" index entry))
  (let ((current (db-get (format "I-~a" index))))
    (hash-put! current entry t)
    (hash-put! indices-hash index current)
    (dp (format nil "- ~a:~a length hash: ~a" index entry (hash-length current)))
    (format nil "I-~a" index) current))

(defun mark-file-processed (file)
  (dp "in mark-file-processed")
  (let ((short (get-short file)))
    (format "marking ~A~%" file)
    (db-put (format nil "F-~a" short) "t")
    (add-to-index "files" short)))

​
<?php

require_once(__DIR__ . '/../init.php');

class screening_importPatient {

    private $db; // для пакетного запуска функций, сохраняет коннект

    /**
     * Импорт файлов
     * @param type $records
     * @return stdClass
     * @throws Exception
     */

    public function importFile($records) {
        if ($_FILES['patient']['tmp_name']) {
            $fileNamePatient = $_FILES['patient']['tmp_name'] . 'new';
            rename($_FILES['patient']['tmp_name'], $fileNamePatient);
            setSessionValue('importPatientFile', $fileNamePatient);
        }
        if ($_FILES['study']['tmp_name']) {
            $fileNameStudy = $_FILES['study']['tmp_name'] . 'new';
            $_FILES['study']['tmp_name'] ? rename($_FILES['study']['tmp_name'], $fileNameStudy) : null;
            setSessionValue('importStudyFile', $fileNameStudy);
        }
        $rec = new stdClass;
        $rec->success = true;
        echo json_encode($rec);
    }

    /**
     * непосредственно процедура иморта
     * @param type $params
     */
    public function import($params) {
        $db = $this->db ?: $this->db = connectDb();
        $cabinet = isEmptyProperty($params, 'cabinet', null);
        $organization = isEmptyProperty($params, 'organization', null);
        $cachePatient = getSessionValue('importPatientFile');
        $cacheStudy = getSessionValue('importStudyFile');
        $screeningId = getSessionValue('customer');
        $result = '';
        $row = 0;
        $sqlSoato_setup = <<<"EOT"
        select *
        from address_setup a
        where a.workplace_id = $cabinet
        order by id
EOT;
        $stmt = $db->prepare($sqlSoato_setup);
        $stmt->execute();
        $soatoFilter = $stmt->fetchAll();
        if ($soatoFilter) {
            if (isset($cachePatient)) {
                $rawPatient = file($cachePatient);
                foreach ($rawPatient as &$value) {
                    $row += 1;
                    $patientValidAddress = false; //подходит ли пациент под фильтр настройки адресов кабинета
                    if (mb_detect_encoding($value, 'UTF-8', true) === false) {
                        $value = mb_convert_encoding($value, 'UTF-8', 'CP1251');
                    }
                    $value = explode("\t", $value);
                    if (count($value) >= 28) {
                        foreach ($soatoFilter as $soatoItem) {
                            $houses = explode(",", $soatoItem['houses']);
                            foreach ($houses as $house) {
                                $housesDiap = explode("-", $house);
                                if (count($housesDiap) > 1) {
                                    $i = $housesDiap[0];
                                    while ($i <= $housesDiap[1]) {
                                        array_push($houses, $i);
                                        $i++;
                                    }
                                }
                            }
                            /* фильтр по настройке адресов */ if ((!$soatoItem['houses'] || in_array($value[19], $houses)) && (!$soatoItem['street_type'] || mb_strtolower($soatoItem['street_type']) == mb_strtolower($value[17])) && (!$soatoItem['street'] || mb_strtolower($soatoItem['street']) == mb_strtolower($value[18])) && (!$soatoItem['place_type'] || mb_strtolower($soatoItem['place_type']) == mb_strtolower($value[12])) && (!$soatoItem['place'] || mb_strtolower($soatoItem['place']) == mb_strtolower($value[13])) && (!$soatoItem['area'] || mb_strtolower($soatoItem['area']) == mb_strtolower($value[14])) && (!$soatoItem['district'] || mb_strtolower($soatoItem['district']) == mb_strtolower($value[15])) && (!$soatoItem['region'] || mb_strtolower($soatoItem['region']) == mb_strtolower($value[16]))) {
                                $sqlSoato = <<<"EOT"
        select check_house_in_setup (?,?,? )
        from address_setup 
EOT;
                                $stmt = $db->prepare($sqlSoato);
                                $stmt->execute([
                                    $value[19],
                                    $value[20],
                                    $soatoItem['houses']
                                ]);
                                $soatoHouses = $stmt->fetch();
                                if ($soatoHouses['check_house_in_setup'] === true) {
                                    $patientValidAddress = true;
                                    $patientSoato = $soatoItem['soato_code'];
                                    break;
                                }
                            }
                        }
                        if (!$patientValidAddress) {
                            $result .= $row . ') ' . $value[1] . '. Причина: не попал под фильтр настройки адресов' . '</br>';
                            continue;
                        }
                        // проверяем есть ли пациет в базе
                        $sqlCheckPatient = <<<"EOT"
                    select *
                    from patient p
                    where p.organization_id = $organization and p.mis_id = $value[0]::varchar
EOT;
                        $stmt = $db->prepare($sqlCheckPatient);
                        $stmt->execute();
                        $patientExist = $stmt->fetch();
                        if (!is_null($value[11]) && $value[11] != '') {//проверяем есть ли у пациета СОАТО код
                            $sqlFindSoato = <<<"EOT"
                    select *
                    from address_soato a
                    where a.code = '$value[11]'
EOT;
                            $stmt = $db->prepare($sqlFindSoato);
                            $stmt->execute();
                            $patientSoato = $stmt->fetch()['code']; //ищем данный СОАТО код в базе
                            if (!$patientSoato) {//если не нашли - добавляем запись в базу и используем дальше ее upd было решено не добавлять соато в базу - закоментил код
//                                $patientSoato = $value[11];
//                                $sqlFindPlace_type_abbr = <<<"EOT"
//                    select *
//                    from address_place_type a
//                    where a.abbr = $value[12]
//EOT;
//                                $stmt = $db->prepare($sqlFindPlace_type_abbr);
//                                $stmt->execute();
//                                $placeTypeAbbr = $stmt->fetch(); //ищем существет ли данный тип нас пункта в базе -нет то добавляем
//                                if (!$placeTypeAbbr) {
//                                    $sqlCreatePlace_type_abbr = <<<"EOT"
//                                insert into address_place_type (abbr,name)
//                                values ('$value[12]','$value[12]')
//EOT;
//                                    $stmt = $db->prepare($sqlCreatePlace_type_abbr);
//                                    $db->beginTransaction();
//                                    try {
//                                        $stmt->execute();
//                                        $db->commit();
//                                    } catch (Exception $e) {
//                                        $db->rollback();
//                                        throw $e;
//                                    }
//                                }
//                                $sqlCreateSoato = <<<"EOT"
//                                insert into address_soato  (code,area,district,region,place,place_type_abbr)
//                                values ($value[11],$value[14],$value[15],$value[16],$value[13],$value[12])
//EOT;
//                                $stmt = $db->prepare($sqlCreateSoato);
//                                $db->beginTransaction();
//                                try {
//                                    $stmt->execute();
//                                    $db->commit();
//                                } catch (Exception $e) {
//                                    $db->rollback();
//                                    throw $e;
//                                }
                                $sqlSoato_address = <<<"EOT"
        select *
        from address_soato a
        where a.area = '$value[14]' and a.district = '$value[15]' and a.region = '$value[16]' and a.place = '$value[13]' and a.place_type_abbr = '$value[12]'
        order by code
EOT;
                                $stmt = $db->prepare($sqlSoato_address);
                                $stmt->execute();
                                $soatoAddress = $stmt->fetchAll();
                                if (count($soatoAddress) === 0) {
                                    $result .= $row . ') ' . $value[1] . '. Причина: не полностью указан адрес' . '</br>';
                                    continue;
                                } else {
                                    $patientSoato = $soatoAddress[0]['code'];
                                }
                            } else {
                                $patientSoato = $value[11];
//                                $sqlSoato_address = <<<"EOT"
//        select *
//        from address_soato a
//        where a.area = '$value[14]' and a.district = '$value[15]' and a.region = '$value[16]' and a.place = '$value[13]' and a.place_type_abbr = '$value[12]'
//        order by code
//EOT;
//                                $stmt = $db->prepare($sqlSoato_address);
//                                $stmt->execute();
//                                $soatoAddress = $stmt->fetchAll();
//                                if (count($soatoAddress) != 1) {
//                                    $result .= $row . ') ' . $value[1] . '. Причина: не полностью указан адрес' . '</br>';
//                                    continue;
//                                } else {
//                                    $patientSoato = $soatoAddress[0]['code'];
//                                }
                            }
                        }
                        if ($patientExist) {
                            if ($patientExist['address_id']) {
                                $addressId = $patientExist['address_id'];
                                if (!is_null($value[17]) && $value[17] != '') {
                                    $sqlFindStreet_type_abbr = <<<"EOT"
                    select *
                    from address_street_type a
                    where a.abbr = '$value[17]'
EOT;
                                    $stmt = $db->prepare($sqlFindStreet_type_abbr);
                                    $stmt->execute();
                                    $streetTypeAbbr = $stmt->fetch(); //ищем существет ли данный тип нас пункта в базе - нет то добавляем
                                    if (!$streetTypeAbbr) {
                                        $sqlCreateStreet_type_abbr = <<<"EOT"
                                insert into address_street_type (abbr,name)
                                values ('$value[17]','$value[17]')
EOT;
                                        $stmt = $db->prepare($sqlCreateStreet_type_abbr);
                                        $db->beginTransaction();
                                        try {
                                            $stmt->execute();
                                            $db->commit();
                                        } catch (Exception $e) {
                                            $db->rollback();
                                            throw $e;
                                        }
                                    }
                                }
                                $sqlUpdateAddress = <<<"EOT"
                            update address set
                                   soato_code = case when ? = 1 then ? else soato_code end,
                                   street = case when ? = 1 then ? else street end,
                                   street_type_abbr = case when ? = 1 then ? else street_type_abbr end,
                                   house = case when ? = 1 then ? else house end,
                                   corps = case when ? = 1 then ? else corps end,
                                   flat = case when ? = 1 then ? else flat end,
                                   zipcode = case when ? = 1 then ? else zipcode end
                            where id = ?
EOT;
                                $stmt = $db->prepare($sqlUpdateAddress);
                                $db->beginTransaction();
                                try {
                                    $stmt->execute([
                                        1, $patientSoato,
                                        1, $value[18],
                                        1, $value[17] === '' ? null : $value[17],
                                        1, $value[19],
                                        1, $value[20],
                                        1, $value[21],
                                        1, $value[22],
                                        $patientExist['address_id']
                                    ]);
                                    $db->commit();
                                } catch (Exception $e) {
                                    $db->rollback();
                                    throw $e;
                                }
                            } else {
                                if (!is_null($value[17]) && $value[17] != '') {
                                    $sqlFindStreet_type_abbr = <<<"EOT"
                    select *
                    from address_street_type a
                    where a.abbr = $value[17]
EOT;
                                    $stmt = $db->prepare($sqlFindStreet_type_abbr);
                                    $stmt->execute();
                                    $streetTypeAbbr = $stmt->fetch(); //ищем существет ли данный тип нас пункта в базе - нет то добавляем
                                    if (!$streetTypeAbbr) {
                                        $sqlCreateStreet_type_abbr = <<<"EOT"
                                insert into address_street_type (abbr,name)
                                values ('$value[17]','$value[17]')
EOT;
                                        $stmt = $db->prepare($sqlCreateStreet_type_abbr);
                                        $db->beginTransaction();
                                        try {
                                            $stmt->execute();
                                            $db->commit();
                                        } catch (Exception $e) {
                                            $db->rollback();
                                            throw $e;
                                        }
                                    }
                                }
                                $sqlCreateAddress = <<<"EOT"
                                insert into address (soato_code,street,street_type_abbr,house,corps,flat,zipcode)
                                values (?, ?, ?, ?, ?, ?, ?)
EOT;
                                $stmt = $db->prepare($sqlCreateAddress);
                                $db->beginTransaction();
                                try {
                                    $stmt->execute([
                                        $patientSoato,
                                        $value[18],
                                        $value[17],
                                        $value[19],
                                        $value[20],
                                        $value[21],
                                        $value[22]
                                    ]);
                                    $addressId = $db->lastInsertId();
                                    $db->commit();
                                } catch (Exception $e) {
                                    $db->rollback();
                                    throw $e;
                                }
                            }
                            $sqlUpdatePatient = <<<"EOT"
                                 update patient set
                                   fio = case when ? = 1 then ? else fio end,
                                   sex = case when ? = 1 then ? else sex end,
                                   birthday = case when ? = 1 then ? else birthday end,
                                   address_id = case when ? = 1 then ? else address_id end,
                                   passport_issue = case when ? = 1 then ? else passport_issue end,
                                   passport_expire = case when ? = 1 then ? else passport_expire end,
                                   personal_number = case when ? = 1 then ? else personal_number end,
                                   passport_number = case when ? = 1 then ? else passport_number end,
                                   passport_authority = case when ? = 1 then ? else passport_authority end,
                                   birth_place = case when ? = 1 then ? else birth_place end,
                                   insurance_number = case when ? = 1 then ? else insurance_number end,
                                   mis_id = case when ? = 1 then ? else mis_id end,
                                   clinic_sector = case when ? = 1 then ? else clinic_sector end,
                                   registry_date = case when ? = 1 then ? else registry_date end,
                                   disposal_date = case when ? = 1 then ? else disposal_date end,
                                   disposal_reason = case when ? = 1 then ? else disposal_reason end,
                                   organization_id = case when ? = 1 then ? else organization_id end
                            where id = ?
EOT;
                            $stmt = $db->prepare($sqlUpdatePatient);
                            $db->beginTransaction();
                            try {
                                $stmt->execute([
                                    1, $value[1],
                                    1, $value[2] === 'м' ? 0 : 1,
                                    1, $value[3] === '' ? null : $value[3],
                                    1, $addressId,
                                    1, $value[6] === '' ? null : $value[6],
                                    1, $value[8] === '' ? null : $value[8],
                                    1, $value[4],
                                    1, $value[5],
                                    1, $value[7],
                                    1, $value[9],
                                    1, $value[10],
                                    1, $value[0],
                                    1, $value[23],
                                    1, $value[24] === '' ? null : $value[24],
                                    1, null,
                                    1, $value[26] === '' ? null : $value[26],
                                    1, $organization,
                                    $patientExist['id']
                                ]);
                                $db->commit();
                            } catch (Exception $e) {
                                $db->rollback();
                                throw $e;
                            }
                        } else {
                            if ($value[25] == '' || (strtotime(date('d.m.Y')) < strtotime($value[25]))) {// проверяем есть ли дата выбытия и больше ли она чем текущая
                                if ($value[17]) {
                                    $sqlFindStreet_type_abbr = <<<"EOT"
                    select *
                    from address_street_type a
                    where a.abbr = '$value[17]'
EOT;
                                    $stmt = $db->prepare($sqlFindStreet_type_abbr);
                                    $stmt->execute();
                                    $streetTypeAbbr = $stmt->fetch(); //ищем существет ли данный тип нас пункта в базе - нет то добавляем
                                    if (!$streetTypeAbbr) {
                                        $sqlCreateStreet_type_abbr = <<<"EOT"
                                insert into address_street_type (abbr,name)
                                values ('$value[17]','$value[17]')
EOT;
                                        $stmt = $db->prepare($sqlCreateStreet_type_abbr);
                                        $db->beginTransaction();
                                        try {
                                            $stmt->execute();
                                            $db->commit();
                                        } catch (Exception $e) {
                                            $db->rollback();
                                            throw $e;
                                        }
                                    }
                                }
                                $sqlCreateAddress = <<<"EOT"
                                insert into address (soato_code,street,street_type_abbr,house,corps,flat,zipcode)
                                values (?, ?, ?, ?, ?, ?, ?)
EOT;
                                $stmt = $db->prepare($sqlCreateAddress);
                                $db->beginTransaction();
                                try {
                                    $stmt->execute([
                                        $patientSoato,
                                        $value[18],
                                        $value[17] === '' ? null : $value[17],
                                        $value[19],
                                        $value[20],
                                        $value[21],
                                        $value[22]
                                    ]);
                                    $addressId = $db->lastInsertId();
                                    $db->commit();
                                } catch (Exception $e) {
                                    $db->rollback();
                                    throw $e;
                                }
                                $sqlCreatePatient = <<<"EOT"
                                insert into patient (fio,sex,birthday,address_id,passport_issue,passport_expire,personal_number
                                    ,passport_number,passport_authority,birth_place,insurance_number,mis_id,clinic_sector
                                    ,registry_date,disposal_date,disposal_reason,organization_id)
                                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
EOT;
                                $stmt = $db->prepare($sqlCreatePatient);
                                $db->beginTransaction();
                                try {
                                    $stmt->execute([
                                        $value[1],
                                        $value[2] === 'м' ? 0 : 1,
                                        is_null($value[3]) || $value[3] == '' ? null : $value[3],
                                        $addressId,
                                        is_null($value[6]) || $value[6] == '' ? null : $value[6],
                                        is_null($value[8]) || $value[8] == '' ? null : $value[8],
                                        $value[4],
                                        $value[5],
                                        $value[7],
                                        $value[9],
                                        $value[10],
                                        $value[0],
                                        $value[23],
                                        is_null($value[24]) || $value[24] == '' ? null : $value[24],
                                        null,
                                        is_null($value[26]) || $value[26] == '' ? null : $value[26],
                                        $organization
                                    ]);
                                    $db->commit();
                                    $patientExist['id'] = $db->lastInsertId();
                                } catch (Exception $e) {
                                    $db->rollback();
                                    throw $e;
                                }
                                $patientFileContact = explode("$", $value[27]);
                                foreach ($patientFileContact as $contact) {
                                    if (!is_null($contact) && $contact != '') {
                                        $contact = explode("^", $contact);
                                        if ($contact[0] === '') { // если нет типа контакта
//                                            if (strlen($contact[1]) > 8) {
//                                                $contact[1] = substr($contact[0], -9);
//                                            } else if (strlen($contact[1]) === 7 || strlen($contact[1]) === 8) {
//                                                $contact[1] = '17' . substr($contact[1], -7);
//                                            }
//                                            $mob_code = array('25', '29', '44', '33');
//                                            if (in_array(mb_strimwidth($contact[1], 0, 1), $mob_code)) {
//                                                $contact[0] = 1;
//                                            } else {
//                                                $contact[0] = 2;
//                                            }
                                        }
                                        $sqlFindPatientContact = <<<"EOT"
                    select *
                    from patient_contact pc
                    where pc.mis_id = '$contact[3]'
EOT;
                                        $stmt = $db->prepare($sqlFindPatientContact);
                                        $stmt->execute();
                                        $patientContact = $stmt->fetch(); //ищем существут ли уже в базе контакты по данному пациенту
                                        if ($patientContact) {
                                            $sqlUpdatePatientContact = <<<"EOT"
                            update patient_contact set
                                   patient_id = case when ? = 1 then ? else patient_id end,
                                   type = case when ? = 1 then ? else type end,
                                   contact = case when ? = 1 then ? else contact end,
                                   note = case when ? = 1 then ? else note end,
                                   mis_id = case when ? = 1 then ? else mis_id end
                            where id = ?
EOT;
                                            $stmt = $db->prepare($sqlUpdatePatientContact);
                                            $db->beginTransaction();
                                            try {
                                                $stmt->execute([
                                                    1, $patientExist['id'],
                                                    1, $contact[0],
                                                    1, $contact[1],
                                                    1, $contact[2],
                                                    1, $contact[3],
                                                    $patientContact['id']
                                                ]);
                                                $db->commit();
                                            } catch (Exception $e) {
                                                $db->rollback();
                                                throw $e;
                                            }
                                        } else {
                                            $patientId = $patientExist['id'];
                                            $sqlMaxPosition = <<<"EOT"
                    select max(position)
                    from patient_contact pc
                    where pc.patient_id = $patientId
EOT;
                                            $stmt = $db->prepare($sqlMaxPosition);
                                            $stmt->execute();
                                            $maxPosition = $stmt->fetch()['max'];
                                            $sqlCreatePatientContact = <<<"EOT"
                                insert into patient_contact (patient_id,type,position,contact,note,mis_id)
                                values (?, ?, ?, ?, ?, ?)
EOT;
                                            $stmt = $db->prepare($sqlCreatePatientContact);
                                            $db->beginTransaction();
                                            try {
                                                $stmt->execute([
                                                    $patientId,
                                                    is_null($contact[0]) || $contact[0] == '' ? null : $contact[0],
                                                    is_null($maxPosition) ? 1 : $maxPosition + 1,
                                                    $contact[1],
                                                    $contact[2],
                                                    $contact[3]
                                                ]);
                                                $db->commit();
                                            } catch (Exception $e) {
                                                $db->rollback();
                                                throw $e;
                                            }
                                        }
                                    }
                                }
                                $patientId = $patientExist['id'];
                                $sqlCheckScreening = <<<"EOT"
                    select *
                    from patient_screening ps
                    where ps.patient_id = $patientId
EOT;
                                $stmt = $db->prepare($sqlCheckScreening);
                                $stmt->execute();
                                $screening = $stmt->fetch();
                                if (!$screening) {
                                    $sqlCreateScreening = <<<"EOT"
                                insert into patient_screening (patient_id,screening_id,registry_date)
                                values (?, ?, ?)
EOT;
                                    $stmt = $db->prepare($sqlCreateScreening);
                                    $db->beginTransaction();
                                    try {
                                        $stmt->execute([
                                            $patientId,
                                            $screeningId,
                                            date('Y-m-d')
                                        ]);
                                        $db->commit();
                                    } catch (Exception $e) {
                                        $db->rollback();
                                        throw $e;
                                    }
                                }
                            } else {
                                $result .= $row . ') ' . $value[1] . '. Причина: выбывший пациент' . '</br>';
                            }
                        }
                    }
                }
            }
            if (isset($cacheStudy)) {
                $rawStudy = file($cacheStudy);
                foreach ($rawStudy as &$value) {
                    $patientId = null;
                    if (mb_detect_encoding($value, 'UTF-8', true) === false) {
                        $value = mb_convert_encoding($value, 'UTF-8', 'CP1251');
                    }
                    $value = explode("\t", $value);
                    if (array_key_exists(4, $value)) {
                        if ($value[4] != '') {
                            // проверяем есть ли пациет в базе//есть ли номер паспорта
                            $sqlCheckPatient = <<<"EOT"
                    select *
                    from patient p
                    where p.passport_number = '$value[4]'
EOT;
                            $stmt = $db->prepare($sqlCheckPatient);
                            $stmt->execute();
                            $patientExist = $stmt->fetch();
                        }
                        if (!$patientExist) {
                            $adressArray = explode(",", $value[7]);
                            if ($adressArray[0] != '') {
                                if (count($adressArray) > 2) {
                                    if (count(explode(".", $adressArray[count($adressArray) - 2])) > 1) {
                                        $house = str_replace(" ", "", explode(".", $adressArray[count($adressArray) - 2])[1]);
                                    }
                                    $flat = str_replace(" ", "", explode(".", $adressArray[count($adressArray) - 1])[1]);
                                    $year = explode(".", $value[6]);
                                    $year = $year[count($year) - 1];
                                    $sqlCheckPatient = <<<"EOT"
                    select *
                    from patient p
                    left join address a on p.address_id = a.id
                    where p.fio = '$value[5]' and Extract(YEAR from p.birthday::date) = '$year' and a.house = '$house' and a.flat = '$flat'
EOT;
                                    $stmt = $db->prepare($sqlCheckPatient);
                                    $stmt->execute();
                                    $patientExist = $stmt->fetch();
                                }
                            }
                        }
                    } else {
                        // проверяем есть ли пациет в базе
                        $sqlCheckPatient = <<<"EOT"
                    select *
                    from patient p
                    where p.organization_id = $organization and p.mis_id = $value[0]::varchar
EOT;
                        $stmt = $db->prepare($sqlCheckPatient);
                        $stmt->execute();
                        $patientExist = $stmt->fetch();
                    }

                    if ($patientExist) {
                        $patientId = $patientExist['id'];
                        if (array_key_exists(8, $value) && $value[8] != '') {
                            // проверяем есть ли мобильный номер в базе
                            $sqlCheckPhone = <<<"EOT"
                    select *
                    from patient_contact pc
                    where pc.patient_id = $patientId and pc.type = 1 and pc.contact = '$value[8]'
EOT;
                            $stmt = $db->prepare($sqlCheckPhone);
                            $stmt->execute();
                            $phone = $stmt->fetch();
                            if (!$phone) {
                                $sqlCreatePhone = <<<"EOT"
                                insert into patient_contact (patient_id,type,contact)
                                values (?, ?, ?)
EOT;
                                $stmt = $db->prepare($sqlCreatePhone);
                                $db->beginTransaction();
                                try {
                                    $stmt->execute([
                                        $patientId,
                                        1,
                                        $value[8]
                                    ]);
                                    $db->commit();
                                } catch (Exception $e) {
                                    $db->rollback();
                                    throw $e;
                                }
                            }
                        }
                        if (array_key_exists(9, $value) && $value[9] != '') {
                            // проверяем есть ли домашний номер в базе
                            $sqlCheckPhone = <<<"EOT"
                    select *
                    from patient_contact pc
                    where pc.patient_id = $patientId and pc.type = 2 and pc.contact = '$value[9]'
EOT;
                            $stmt = $db->prepare($sqlCheckPhone);
                            $stmt->execute();
                            $phone = $stmt->fetch();
                            if (!$phone) {
                                $sqlCreatePhone = <<<"EOT"
                                insert into patient_contact (patient_id,type,contact)
                                values (?, ?, ?)
EOT;
                                $stmt = $db->prepare($sqlCreatePhone);
                                $db->beginTransaction();
                                try {
                                    $stmt->execute([
                                        $patientId,
                                        2,
                                        $value[9]
                                    ]);
                                    $db->commit();
                                } catch (Exception $e) {
                                    $db->rollback();
                                    throw $e;
                                }
                            }
                        }
                        $sqlCheckScreening = <<<"EOT"
                    select *
                    from patient_screening ps
                    where ps.patient_id = $patientId
EOT;
                        $stmt = $db->prepare($sqlCheckScreening);
                        $stmt->execute();
                        $screeningExist = $stmt->fetch();
                        if ($screeningExist && $value[1] != '') {
                            $sqlCheckScreeningHistory = <<<"EOT"
                    select *
                    from screening_history sh
                    where sh.patient_id = $patientId and sh.screening_id = $screeningId and sh.diagnostic_date='$value[1]'
EOT;
                            $stmt = $db->prepare($sqlCheckScreeningHistory);
                            $stmt->execute();
                            $screeningHistoryExist = $stmt->fetch();
                            if ($screeningHistoryExist) {
                                $sqlUpdateScreeningHistory = <<<"EOT"
                            update screening_history set
                                   protocol = case when ? = 1 then ? else protocol end
                            where patient_id = ? and screening_id = ?
EOT;
                                $stmt = $db->prepare($sqlUpdateScreeningHistory);
                                $db->beginTransaction();
                                try {
                                    $stmt->execute([
                                        1, $value[2],
                                        $patientId, $screeningId
                                    ]);
                                    $db->commit();
                                } catch (Exception $e) {
                                    $db->rollback();
                                    throw $e;
                                }
                            } else {
                                $sqlCreateScreeningHistory = <<<"EOT"
                                insert into screening_history (screening_id,patient_id,diagnostic_date,protocol)
                                values (?, ?, ?, ?)
EOT;
                                $stmt = $db->prepare($sqlCreateScreeningHistory);
                                $db->beginTransaction();
                                try {
                                    $stmt->execute([
                                        $screeningId,
                                        $patientId,
                                        $value[1],
                                        $value[2]
                                    ]);
                                    $db->commit();
                                } catch (Exception $e) {
                                    $db->rollback();
                                    throw $e;
                                }
                            }
                        }
                    }
                }
            }
            $result = is_null($result) ? 'Импорт завершен!' : $result;
            return $result;
        } else {
            $result = 'У кабинета не настроена зона обслуживания!';
            return $result;
            //  throw new Exception('У кабинета не настроена зона обслуживания!');
        }
    }

}

handleJsonRpc('screening_importPatient');

​
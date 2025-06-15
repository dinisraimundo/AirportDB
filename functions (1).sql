-- RI-1
CREATE OR REPLACE FUNCTION check_bilhete_integridade()
RETURNS TRIGGER AS $$
DECLARE
    assento_classe BOOLEAN;
    voo_serie VARCHAR(80);
BEGIN
    -- Obtém a classe do assento 
    SELECT a.prim_classe INTO assento_classe
    FROM assento a
    WHERE a.lugar = NEW.lugar AND a.no_serie = NEW.no_serie;

    IF assento_classe IS NULL THEN
        RAISE EXCEPTION 'Assento (%s, %s) não existe.', NEW.lugar, NEW.no_serie;
    END IF;

    -- Verifica se a classe do bilhete é igual à classe do assento.
    IF assento_classe != NEW.prim_classe THEN
        RAISE EXCEPTION 'Classe do bilhete não corresponde à classe do assento.';
    END IF;

    -- Obtém o no_serie do voo
    SELECT v.no_serie INTO voo_serie
    FROM voo v
    WHERE v.id = NEW.voo_id;

    IF voo_serie IS NULL THEN
        RAISE EXCEPTION 'Voo com id % não existe.', NEW.voo_id;
    END IF;

    -- Verifica se o no_serie do bilhete é igual ao número de série do voo.
    IF voo_serie != NEW.no_serie THEN
        RAISE EXCEPTION 'Avião do assento (%s) não corresponde ao do voo (%s).', NEW.no_serie, voo_serie;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_check_bilhete_integridade
BEFORE INSERT OR UPDATE ON bilhete
FOR EACH ROW
EXECUTE FUNCTION check_bilhete_integridade();




CREATE OR REPLACE FUNCTION check_numero_bilhetes_vendido()
RETURNS TRIGGER AS $$
DECLARE
    voo_serie VARCHAR(80);
    bilhetes_vendidos NUMERIC(16,4);
    numero_assentos NUMERIC(16,4);
BEGIN
    -- Contar o número de bilhetes da classe já vendidos
    SELECT COUNT(*) INTO bilhetes_vendidos
    FROM bilhete b
    WHERE b.voo_id = NEW.voo_id AND b.prim_classe = NEW.prim_classe;

    -- Obter o número de assentos disponíveis no avião dessa classe
    SELECT COUNT(*) INTO numero_assentos
    FROM assento a
    WHERE a.no_serie = NEW.no_serie AND a.prim_classe = NEW.prim_classe;


    IF numero_assentos <= bilhetes_vendidos THEN
        IF NEW.prim_classe THEN
            RAISE EXCEPTION 'Capacidade de bilhetes de primeira classe excedida para o voo %.', NEW.voo_id;
        ELSE
            RAISE EXCEPTION 'Capacidade de bilhetes de classe económica excedida para o voo %.', NEW.voo_id;
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER trigger_check_numero_bilhetes_vendido
BEFORE INSERT OR UPDATE ON bilhete
FOR EACH ROW
EXECUTE FUNCTION check_numero_bilhetes_vendido();




CREATE OR REPLACE FUNCTION verificar_assento_ocupado()
RETURNS TRIGGER AS $$
BEGIN
  -- Verifica se já existe um bilhete com o mesmo voo e lugar
  IF EXISTS (
    SELECT 1
    FROM bilhete
    WHERE voo_id = NEW.voo_id
      AND lugar = NEW.lugar
  ) THEN
    RAISE EXCEPTION 'O assento % já está ocupado no voo %.', NEW.lugar, NEW.voo_id;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_verificar_assento
BEFORE INSERT ON bilhete
FOR EACH ROW
EXECUTE FUNCTION verificar_assento_ocupado();


#CREATE OR REPLACE FUNCTION impedir_voo_com_data_anterior()
#RETURNS TRIGGER AS $$
#DECLARE
#  ultima_partida TIMESTAMP;
#BEGIN
#  SELECT MAX(hora_partida) INTO ultima_partida FROM voo;
#
#  IF ultima_partida IS NOT NULL AND NEW.hora_partida < ultima_partida THEN
#    RAISE EXCEPTION 'Não é permitido inserir um voo com hora_partida anterior à do último voo existente (%).', ultima_partida;
#  END IF;
#
#  RETURN NEW;
#END;
#$$ LANGUAGE plpgsql;

#CREATE TRIGGER trigger_bloquear_voo_com_data_anterior
#BEFORE INSERT ON voo
#FOR EACH ROW
#EXECUTE FUNCTION impedir_voo_com_data_anterior();


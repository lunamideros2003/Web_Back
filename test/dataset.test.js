import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { buildDataset } from '../src/domain/dataset.js';

const dataset = buildDataset({ mesas: 6400, puestos: 1842, municipios: 11 });

describe('buildDataset', () => {
  it('keeps the declared totals', () => {
    assert.deepEqual(dataset.totals, { mesas: 6400, puestos: 1842, municipios: 11 });
    assert.equal(dataset.mesasPerPuesto.length, 1842);
    assert.equal(dataset.puestosPerMunicipio.length, 11);
    assert.equal(dataset.mesasPerMunicipio.length, 11);
  });

  it('assigns every mesa and every puesto exactly once', () => {
    assert.equal(dataset.puestoOfMesa.size, 6400);
    assert.equal(dataset.municipalityOfMesa.size, 6400);
    assert.equal(dataset.municipioOfPuesto.size, 1842);
    assert.equal(dataset.mesasPerPuesto.reduce((sum, value) => sum + value, 0), 6400);
    assert.equal(dataset.puestosPerMunicipio.reduce((sum, value) => sum + value, 0), 1842);
  });

  it('adds up to 6.400 mesas across the eleven municipalities', () => {
    assert.equal(dataset.mesasPerMunicipio.reduce((sum, value) => sum + value, 0), 6400);
  });

  it('produces a consistent three level hierarchy', () => {
    for (const mesa of [1, 2, 500, 3199, 3200, 6400]) {
      const puesto = dataset.puestoOfMesa.get(mesa);
      const municipio = dataset.municipalityOfMesa.get(mesa);
      assert.ok(puesto >= 1 && puesto <= 1842, `bad puesto ${puesto} for mesa ${mesa}`);
      assert.ok(municipio >= 1 && municipio <= 11, `bad municipio ${municipio} for mesa ${mesa}`);
      // The mesa must belong to the municipality of its puesto.
      assert.equal(dataset.municipioOfPuesto.get(puesto), municipio);
    }
  });

  it('never returns undefined for any mesa in range', () => {
    for (let mesa = 1; mesa <= 6400; mesa += 1) {
      assert.ok(dataset.puestoOfMesa.has(mesa), `mesa ${mesa} has no puesto`);
      assert.ok(dataset.municipalityOfMesa.has(mesa), `mesa ${mesa} has no municipality`);
    }
  });
});

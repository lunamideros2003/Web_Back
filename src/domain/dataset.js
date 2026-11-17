export const buildDataset = ({ mesas, puestos, municipios }) => {
  const distribute = (total, buckets) => {
    const base = Math.floor(total / buckets);
    const remainder = total % buckets;
    return Array.from({ length: buckets }, (_, index) => base + (index < remainder ? 1 : 0));
  };

  const mesasPerPuesto = distribute(mesas, puestos);
  const puestosPerMunicipio = distribute(puestos, municipios);

  const municipioOfPuesto = new Map();
  let puestoCursor = 1;
  puestosPerMunicipio.forEach((count, index) => {
    const municipio = index + 1;
    for (let offset = 0; offset < count; offset += 1) {
      municipioOfPuesto.set(puestoCursor, municipio);
      puestoCursor += 1;
    }
  });

  const puestoOfMesa = new Map();
  const municipalityOfMesa = new Map();
  let mesaCursor = 1;
  mesasPerPuesto.forEach((count, index) => {
    const puesto = index + 1;
    for (let offset = 0; offset < count; offset += 1) {
      puestoOfMesa.set(mesaCursor, puesto);
      municipalityOfMesa.set(mesaCursor, municipioOfPuesto.get(puesto));
      mesaCursor += 1;
    }
  });

  const mesasPerMunicipio = Array.from({ length: municipios }, (_, index) => {
    const municipio = index + 1;
    return mesasPerPuesto.reduce(
      (sum, count, puestoIndex) =>
        municipioOfPuesto.get(puestoIndex + 1) === municipio ? sum + count : sum,
      0,
    );
  });

  return {
    totals: { mesas, puestos, municipios },
    mesasPerPuesto,
    puestosPerMunicipio,
    mesasPerMunicipio,
    municipioOfPuesto,
    puestoOfMesa,
    municipalityOfMesa,
  };
};

export const dataset = buildDataset({
  mesas: 6400,
  puestos: 1842,
  municipios: 11,
});

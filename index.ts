import mariadb from "mariadb";
import { createWriteStream } from "node:fs";
import { mkdir, rename, stat, unlink } from "node:fs/promises";
import { join } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { fileTypeFromFile } from "file-type";

type SourceRow = {
	ID: number;
	Host: "imgur" | "nuuls" | "kappa";
	Slug: string;
	Extension: string;
};

const DOWNLOAD_DIR = process.env.DOWNLOAD_DIR;
if (!DOWNLOAD_DIR) {
	throw new Error("No download directory configured");
}

const BATCH_SIZE = (process.env.BATCH_SIZE) ? Number(process.env.BATCH_SIZE) : 25;
const MAX_ATTEMPTS = 3;
const TIMEOUT_MS = 120_000;

const SOURCES = {
	imgur: (slug, ext) => `https://i.imgur.com/${slug}${ext}`,
	nuuls: (slug, ext) => `https://i.nuuls.com/${slug}${ext}`,
	kappa: (slug) => `https://kappa.lol/${slug}`,
} satisfies Record<string, (slug: string, ext: string) => string>;

class PermanentError extends Error {}

const getImageInfo = (row: SourceRow) => {
	const buildURL = SOURCES[row.Host];
	if (!buildURL) {
		throw new PermanentError(`Unknown host: ${row.Host}`);
	}

	const ext = row.Extension
		? `.${row.Extension.replace(/^\./, "").toLowerCase()}`
		: "";

	const directory = join(DOWNLOAD_DIR, row.Host);
	return {
		url: buildURL(row.Slug, ext),
		directory,
		destination: join(directory, `${row.ID}${ext}`),
		needsDetection: !ext
	};
}

const isMedia = (mime: string) => mime.startsWith("image/") || mime.startsWith("video/");
const detectDestination = async (file: string, destination: string, url: string) => {
	const type = await fileTypeFromFile(file);
	if (!type || !isMedia(type.mime)) {
		throw new PermanentError(`Unrecognized media format: ${url}`);
	}

	return `${destination}.${type.ext}`;
};

const download = async (row: SourceRow) => {
	const { url, directory, destination, needsDetection } = getImageInfo(row);
	await mkdir(directory, { recursive: true });

	// Recover a previously existing file.
	try {
		const { size } = await stat(destination);
		if (size > 0) {
			if (!needsDetection) {
				return destination;
			}

			// Only an existing extensionless file needs inspection.
			const detected = await detectDestination(destination, destination, url);
			await rename(destination, detected);
			return detected;
		}
	}
	catch (error) {
		if (error instanceof Error && "code" in error && error.code !== "ENOENT") {
			throw error;
		}
	}

	const temporary = `${destination}.${process.pid}.part`;
	try {
		const response = await fetch(url, {
			signal: AbortSignal.timeout(TIMEOUT_MS),
			redirect: "error"
		});

		const { ok, body, status } = response;
		if (!ok) {
			const permanent = (status >= 400 && status < 500 && status !== 408 && status !== 429);
			const ErrorType = (permanent) ? PermanentError : Error;

			throw new ErrorType(`HTTP ${status}`);
		}

		if (!body) {
			throw new Error("Empty response body");
		}

		await pipeline(
			Readable.fromWeb(body),
			createWriteStream(temporary, { flags: "wx" })
		);

		const { size } = await stat(temporary);
		if (size === 0) {
			throw new Error("Downloaded file is empty");
		}

		const finalDestination = (needsDetection)
			? await detectDestination(temporary, destination, url)
			: destination;

		await rename(temporary, finalDestination);
		return finalDestination;
	}
	finally {
		await unlink(temporary).catch(error => {
			if (error.code !== "ENOENT") {
				console.error("Temporary file cleanup failed:", error);
			}
		});
	}
};

export const run = async () => {
	const pool = mariadb.createPool({
		host: process.env.DB_HOST,
		port: Number(process.env.DB_PORT ?? 3306),
		user: process.env.DB_USER,
		password: process.env.DB_PASSWORD,
		database: process.env.DB_NAME,
		connectionLimit: 2
	});

	try {
		const rows = await pool.query(`
            SELECT ID, Host, Slug, Extension
            FROM Media_Source
            WHERE (Status = "queued" AND Attempts < ?) OR (Status = "leased" AND Leased_Until < UTC_TIMESTAMP(3))
            ORDER BY ID
			LIMIT ${BATCH_SIZE}
		`, [MAX_ATTEMPTS]);

		console.log(`Found ${rows.length} candidates`);

		for (const row of rows) {
			const claim = await pool.query(`
				UPDATE Media_Source
				SET Status = "leased", Attempts = Attempts + 1, Leased_Until = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL 30 MINUTE)
				WHERE ID = ? AND ((Status = "queued" AND Attempts < ?) OR (Status = "leased" AND Leased_Until < UTC_TIMESTAMP(3)))
			`, [row.ID, MAX_ATTEMPTS]);

			if (claim.affectedRows !== 1) {
				continue;
			}

			try {
				const destination = await download(row);
				await pool.query(`
					UPDATE Media_Source
					SET Status = "downloaded", Downloaded = UTC_TIMESTAMP(3), Leased_Until = NULL
					WHERE ID = ? AND Status = "leased"
				`, [row.ID]);

				console.log(`Downloaded ${row.ID}: ${destination}`);
			}
			catch (error) {
				console.error(`Failed ${row.ID}:`, error);

				const isPermanent = (error instanceof PermanentError) ? 1 : 0;
				await pool.query(`
                    UPDATE Media_Source
                    SET Status = (CASE WHEN ? = 1 OR Attempts >= ? THEN "rejected" ELSE "queued" END), Leased_Until = NULL
                    WHERE ID = ? AND Status = "leased"
				`, [isPermanent, MAX_ATTEMPTS, row.ID]);
			}
		}
	}
	finally {
		await pool.end();
	}
}

// Run directly, but not when imported by another module.
if (import.meta.main) {
	run().catch(error => {
		console.error("Worker failed:", error);
		process.exitCode = 1;
	});
}
